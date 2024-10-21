// Copyright 2017 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.worker;

import static build.buildfarm.common.Claim.Stage.EXECUTE_ACTION_STAGE;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Maps.uniqueIndex;
import static com.google.protobuf.util.Durations.add;
import static com.google.protobuf.util.Durations.compare;
import static com.google.protobuf.util.Durations.fromSeconds;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Command.EnvironmentVariable;
import build.bazel.remote.execution.v2.ExecutionStage;
import build.bazel.remote.execution.v2.Platform.Property;
import build.buildfarm.common.Claim;
import build.buildfarm.common.ProcessUtils;
import build.buildfarm.common.Time;
import build.buildfarm.common.Write;
import build.buildfarm.common.Write.NullWrite;
import build.buildfarm.common.config.BuildfarmConfigs;
import build.buildfarm.common.config.ExecutionPolicy;
import build.buildfarm.common.config.ExecutionWrapper;
import build.buildfarm.v1test.Tree;
import build.buildfarm.worker.WorkerContext.IOResource;
import build.buildfarm.worker.persistent.PersistentExecutor;
import build.buildfarm.worker.persistent.WorkFilesContext;
import build.buildfarm.worker.resources.ResourceLimits;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.core.DockerClientBuilder;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.devtools.build.lib.shell.Protos.ExecutionStatistics;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import com.google.rpc.Code;
import io.grpc.Deadline;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;
import lombok.extern.java.Log;

@Log
class Executor {
  private static final int INCOMPLETE_EXIT_CODE = -1;

  private final WorkerContext workerContext;
  private final ExecutionContext executionContext;
  private final ExecuteActionStage owner;
  private final java.util.concurrent.Executor pollerExecutor;
  private int exitCode = INCOMPLETE_EXIT_CODE;
  private boolean wasErrored = false;

  Executor(
      WorkerContext workerContext,
      ExecutionContext executionContext,
      ExecuteActionStage owner,
      java.util.concurrent.Executor pollerExecutor) {
    this.workerContext = workerContext;
    this.executionContext = executionContext;
    this.owner = owner;
    this.pollerExecutor = pollerExecutor;
  }

  // ensure that only one error put attempt occurs
  private void putError() throws InterruptedException {
    if (!wasErrored) {
      wasErrored = true;
      owner.error().put(executionContext);
    }
  }

  private boolean putOperation(boolean ignoreFailure) throws InterruptedException {
    Operation operation =
        executionContext.operation.toBuilder()
            .setMetadata(Any.pack(executionContext.metadata.build()))
            .build();

    boolean operationUpdateSuccess = false;
    try {
      operationUpdateSuccess = workerContext.putOperation(operation);
    } catch (IOException e) {
      log.log(
          Level.SEVERE, format("error putting operation %s as EXECUTING", operation.getName()), e);
    }

    if (!operationUpdateSuccess) {
      log.log(
          Level.WARNING,
          String.format(
              "InputFetcher::run(%s): could not transition to EXECUTING", operation.getName()));
      if (!ignoreFailure) {
        putError();
      }
    }
    return operationUpdateSuccess;
  }

  private long runInterruptible(Stopwatch stopwatch, ResourceLimits limits)
      throws InterruptedException {
    Timestamp executionStartTimestamp = Timestamps.now();

    executionContext
        .metadata
        .getExecuteOperationMetadataBuilder()
        .setStage(ExecutionStage.Value.EXECUTING)
        .getPartialExecutionMetadataBuilder()
        .setExecutionStartTimestamp(executionStartTimestamp);
    if (!putOperation(/* ignoreFailure= */ false)) {
      return 0;
    }

    // settings for deciding timeout
    TimeoutSettings timeoutSettings = new TimeoutSettings();
    timeoutSettings.defaultTimeout = workerContext.getDefaultActionTimeout();
    timeoutSettings.maxTimeout = workerContext.getMaximumActionTimeout();

    // decide timeout and begin deadline
    Duration timeout = decideTimeout(timeoutSettings, executionContext.action);
    Deadline pollDeadline = Time.toDeadline(timeout).offset(30, TimeUnit.SECONDS);

    workerContext.resumePoller(
        executionContext.poller,
        "Executor",
        executionContext.queueEntry,
        ExecutionStage.Value.EXECUTING,
        Thread.currentThread()::interrupt,
        pollDeadline,
        pollerExecutor);

    Iterable<ExecutionPolicy> policies = new ArrayList<>();
    if (limits.useExecutionPolicies) {
      policies =
          ExecutionPolicies.forPlatform(
              executionContext.command.getPlatform(), workerContext::getExecutionPolicies);
    }
    // since pools mandate injection of resources, each pool claim must require a policy
    // could probably do this with just the properties and the pool resource sets, but this is
    // faster
    for (String pool : Iterables.transform(executionContext.claim.getPools(), Map.Entry::getKey)) {
      policies = Iterables.concat(policies, workerContext.getExecutionPolicies("pool-" + pool));
    }

    try {
      return executePolled(limits, policies, timeout, stopwatch);
    } finally {
      executionContext.poller.pause();
    }
  }

  private static Duration decideTimeout(TimeoutSettings settings, Action action) {
    // First we need to acquire the appropriate timeout duration for the action.
    // We begin with a default configured timeout.
    Duration timeout = settings.defaultTimeout;

    // Typically the timeout comes from the client as a part of the action.
    // We will use this if the client has provided a value.
    if (action.hasTimeout()) {
      timeout = action.getTimeout();
    }

    // Now that a timeout is chosen, it may be adjusted further based on execution considerations.
    // For example, an additional padding time may be added to guarantee resource cleanup around the
    // action's execution.
    if (settings.applyTimeoutPadding) {
      timeout = add(timeout, fromSeconds(settings.timeoutPaddingSeconds));
    }

    // Ensure the timeout is not too long by comparing it to the maximum allowed timeout
    if (compare(timeout, settings.maxTimeout) > 0) {
      timeout = settings.maxTimeout;
    }

    return timeout;
  }

  private static final class Interpolator {
    private final Iterable<?> value;

    Interpolator(String value) {
      this(ImmutableList.of(value));
    }

    Interpolator(Iterable<?> value) {
      this.value = value;
    }

    void inject(Consumer<String> consumer) {
      Iterables.transform(value, String::valueOf).forEach(consumer);
    }
  }

  private static Map<String, Interpolator> createInterpolations(
      Claim claim, Iterable<Property> properties) {
    Map<String, Interpolator> interpolations = new HashMap<>();
    if (claim != null) {
      for (Map.Entry<String, List<Object>> pool : claim.getPools()) {
        List<Object> ids = pool.getValue();
        interpolations.put(pool.getKey(), new Interpolator(ids));
        for (int i = 0; i < ids.size(); i++) {
          interpolations.put(pool.getKey() + "-" + i, new Interpolator(String.valueOf(ids.get(i))));
        }
      }
    }
    interpolations.putAll(
        transformValues(
            uniqueIndex(properties, Property::getName),
            property -> new Interpolator(property.getValue())));
    return interpolations;
  }

  private static Iterable<String> transformWrapper(
      ExecutionWrapper wrapper, Map<String, Interpolator> interpolations) {
    ImmutableList.Builder<String> arguments = ImmutableList.builder();

    arguments.add(wrapper.getPath());

    if (wrapper.getArguments() != null) {
      for (String argument : wrapper.getArguments()) {
        // If the argument is of the form <propertyName>, substitute the value of
        // the property from the platform specification.
        if (!argument.equals("<>")
            && argument.charAt(0) == '<'
            && argument.charAt(argument.length() - 1) == '>') {
          // substitute with matching interpolation
          // if this property is not present, the wrapper is ignored
          String name = argument.substring(1, argument.length() - 1);
          Interpolator interpolator = interpolations.get(name);
          if (interpolator == null) {
            return ImmutableList.of();
          }
          interpolator.inject(arguments::add);
        } else {
          // If the argument isn't of the form <propertyName>, add the argument directly:
          arguments.add(argument);
        }
      }
    }

    return arguments.build();
  }

  private long executePolled(
      ResourceLimits limits,
      Iterable<ExecutionPolicy> policies,
      Duration timeout,
      Stopwatch stopwatch)
      throws InterruptedException {
    /* execute command */
    String operationName = executionContext.operation.getName();
    log.log(Level.FINER, "Executor: Operation " + operationName + " Executing command");

    Command command = executionContext.command;
    Path workingDirectory = executionContext.execDir;
    if (!command.getWorkingDirectory().isEmpty()) {
      workingDirectory = workingDirectory.resolve(command.getWorkingDirectory());
    }

    // similar to the policy selection here
    Map<String, Interpolator> interpolations =
        createInterpolations(
            executionContext.claim, executionContext.command.getPlatform().getPropertiesList());

    ImmutableList.Builder<String> arguments = ImmutableList.builder();

    for (ExecutionPolicy policy : policies) {
      if (policy.getExecutionWrapper() != null) {
        arguments.addAll(transformWrapper(policy.getExecutionWrapper(), interpolations));
      }
    }

    Code statusCode;
    try (IOResource resource =
        workerContext.limitExecution(
            operationName,
            executionContext.claim.owner(),
            arguments,
            executionContext.command,
            workingDirectory)) {
      // Windows requires that relative command programs are absolutized
      Iterator<String> argumentItr = command.getArgumentsList().iterator();
      boolean absolutizeExe =
          BuildfarmConfigs.getInstance().getWorker().isAbsolutizeCommandProgram()
              && argumentItr.hasNext()
              && Files.exists(workingDirectory.resolve(command.getArguments(0)));
      if (absolutizeExe) {
        Path exe =
            workingDirectory.resolve(
                argumentItr.next()); // Get first element, this is the executable
        arguments.add(exe.toAbsolutePath().normalize().toString());
      }
      argumentItr.forEachRemaining(arguments::add);

      statusCode =
          executeCommand(
              operationName,
              workingDirectory,
              arguments.build(),
              command.getEnvironmentVariablesList(),
              limits,
              timeout,
              // executingMetadata.getStdoutStreamName(),
              // executingMetadata.getStderrStreamName(),
              executionContext.executeResponse.getResultBuilder());

      // From Bazel Test Encyclopedia:
      // If the main process of a test exits, but some of its children are still running,
      // the test runner should consider the run complete and count it as a success or failure
      // based on the exit code observed from the main process. The test runner may kill any stray
      // processes. Tests should not leak processes in this fashion.
      // Based on configuration, we will decide whether remaining resources should be an error.
      if (workerContext.shouldErrorOperationOnRemainingResources()
          && resource.isReferenced()
          && statusCode == Code.OK) {
        // there should no longer be any references to the resource. Any references will be
        // killed upon close, but we must error the operation due to improper execution
        // per the gRPC spec: 'The operation was attempted past the valid range.' Seems
        // appropriate
        statusCode = Code.OUT_OF_RANGE;
        executionContext
            .executeResponse
            .getStatusBuilder()
            .setMessage("command resources were referenced after execution completed");
      }
    } catch (IOException e) {
      log.log(Level.SEVERE, format("error executing operation %s", operationName), e);
      executionContext.poller.pause();
      putError();
      return 0;
    }

    // switch poller to disable deadline
    executionContext.poller.pause();
    workerContext.resumePoller(
        executionContext.poller,
        "Executor(claim)",
        executionContext.queueEntry,
        ExecutionStage.Value.EXECUTING,
        Thread.currentThread()::interrupt,
        Deadline.after(10, DAYS),
        pollerExecutor);

    long executeUSecs = stopwatch.elapsed(MICROSECONDS);

    executionContext
        .metadata
        .getExecuteOperationMetadataBuilder()
        .getPartialExecutionMetadataBuilder()
        .setExecutionCompletedTimestamp(Timestamps.now());
    putOperation(/* ignoreFailure= */ true);

    log.log(
        Level.FINER,
        String.format(
            "Executor::executeCommand(%s): Completed command: exit code %d",
            operationName, executionContext.executeResponse.getResultBuilder().getExitCode()));

    executionContext.executeResponse.getStatusBuilder().setCode(statusCode.getNumber());
    boolean claimed = owner.output().claim(executionContext);
    executionContext.poller.pause();
    if (claimed) {
      try {
        owner.output().put(executionContext);
      } catch (InterruptedException e) {
        owner.output().release();
        throw e;
      }
    } else {
      log.log(Level.FINER, "Executor: Operation " + operationName + " Failed to claim output");
      boolean wasInterrupted = Thread.interrupted();
      try {
        putError();
      } finally {
        if (wasInterrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
    return stopwatch.elapsed(MICROSECONDS) - executeUSecs;
  }

  public void run(ResourceLimits limits) {
    long stallUSecs = 0;
    Stopwatch stopwatch = Stopwatch.createStarted();
    String operationName = executionContext.operation.getName();
    try {
      stallUSecs = runInterruptible(stopwatch, limits);
    } catch (InterruptedException e) {
      /* we can be interrupted when the poller fails */
      try {
        putError();
      } catch (InterruptedException errorEx) {
        log.log(Level.SEVERE, format("interrupted while erroring %s", operationName), errorEx);
      } finally {
        Thread.currentThread().interrupt();
      }
    } catch (Exception e) {
      // clear interrupt flag for error put
      boolean wasInterrupted = Thread.interrupted();
      log.log(Level.SEVERE, format("errored during execution of %s", operationName), e);
      try {
        putError();
      } catch (InterruptedException errorEx) {
        log.log(
            Level.SEVERE,
            format("interrupted while erroring %s after error", operationName),
            errorEx);
      } catch (Exception errorEx) {
        log.log(
            Level.SEVERE, format("errored while erroring %s after error", operationName), errorEx);
      }
      if (wasInterrupted) {
        Thread.currentThread().interrupt();
      }
      throw e;
    } finally {
      boolean wasInterrupted = Thread.interrupted();
      try {
        // Now that the execution has finished we can return any of the claims against local
        // resources.
        executionContext.claim.release(EXECUTE_ACTION_STAGE);
        owner.releaseExecutor(
            operationName,
            limits.cpu.claimed,
            stopwatch.elapsed(MICROSECONDS),
            stallUSecs,
            exitCode);
      } finally {
        if (wasInterrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  /**
   * Decide if this action should run on a Persistent Worker. <br>
   *
   * <ul>
   *   <li>The Persistent worker key must be set. This is set by bazel client with
   *       "--experimental_remote_mark_tool_inputs"
   *   <li>the config.yaml has to have a "*" OR have the mnemonic allowlisted.
   * </ul>
   *
   * @return <c>true</c> if the action should be sent to a persistent worker, <c>false</c>
   *     otherwise.
   */
  private boolean shouldRunOnPersistentWorker(ResourceLimits limits) {
    if (!limits.persistentWorkerKey.isEmpty()) {
      Collection<String> mnemonicAllowlist =
          BuildfarmConfigs.getInstance().getWorker().getPersistentWorkerActionMnemonicAllowlist();
      String actionMnemonic = executionContext.metadata.getRequestMetadata().getActionMnemonic();
      return mnemonicAllowlist.contains("*") || mnemonicAllowlist.contains(actionMnemonic);
    }
    return false;
  }

  @SuppressWarnings("ConstantConditions")
  private Code executeCommand(
      String operationName,
      Path execDir,
      List<String> arguments,
      List<EnvironmentVariable> environmentVariables,
      ResourceLimits limits,
      Duration timeout,
      ActionResult.Builder resultBuilder)
      throws IOException, InterruptedException {
    ProcessBuilder processBuilder =
        new ProcessBuilder(arguments).directory(execDir.toAbsolutePath().toFile());

    Map<String, String> environment = processBuilder.environment();
    environment.clear();
    for (EnvironmentVariable environmentVariable : environmentVariables) {
      environment.put(environmentVariable.getName(), environmentVariable.getValue());
    }
    environment.putAll(limits.extraEnvironmentVariables);

    // allow debugging before an execution
    if (limits.debugBeforeExecution) {
      return ExecutionDebugger.performBeforeExecutionDebug(processBuilder, limits, resultBuilder);
    }
    if (shouldRunOnPersistentWorker(limits)) {
      // RBE Client suggests to run this Action as persistent...
      log.fine(
          "usePersistentWorker (mnemonic="
              + executionContext.metadata.getRequestMetadata().getActionMnemonic()
              + ")");

      Tree execTree = executionContext.tree;

      WorkFilesContext filesContext =
          WorkFilesContext.fromContext(execDir, execTree, executionContext.command);

      return PersistentExecutor.runOnPersistentWorker(
          filesContext,
          operationName,
          ImmutableList.copyOf(arguments),
          ImmutableMap.copyOf(environment),
          limits,
          timeout,
          PersistentExecutor.defaultWorkRootsDir,
          resultBuilder);
    }

    // run the action under docker
    if (limits.containerSettings.enabled) {
      DockerClient dockerClient = DockerClientBuilder.getInstance().build();

      // create settings
      DockerExecutorSettings settings = new DockerExecutorSettings();
      settings.fetchTimeout = Durations.fromMinutes(1);
      settings.executionContext = executionContext;
      settings.execDir = execDir;
      settings.limits = limits;
      settings.envVars = environment;
      settings.timeout = timeout;
      settings.arguments = arguments;

      return DockerExecutor.runActionWithDocker(dockerClient, settings, resultBuilder);
    }
    long startNanoTime = System.nanoTime();
    Process process;
    try {
      process = ProcessUtils.threadSafeStart(processBuilder);
      process.getOutputStream().close();
    } catch (IOException e) {
      log.log(Level.SEVERE, format("error starting process for %s", operationName), e);
      // again, should we do something else here??
      resultBuilder.setExitCode(INCOMPLETE_EXIT_CODE);
      // The openjdk IOException for an exec failure here includes the working
      // directory of the execution. Drop it and reconstruct without it if we
      // can get the cause.
      Throwable t = e.getCause();
      String message;
      if (t != null) {
        message =
            "Cannot run program \"" + processBuilder.command().getFirst() + "\": " + t.getMessage();
      } else {
        message = e.getMessage();
      }
      resultBuilder.setStderrRaw(ByteString.copyFromUtf8(message));
      return Code.INVALID_ARGUMENT;
    }

    // Create threads to extract stdout/stderr from a process.
    // The readers attach to the process's input/error streams.
    final Write stdoutWrite = new NullWrite();
    final Write stderrWrite = new NullWrite();
    ByteStringWriteReader stdoutReader =
        new ByteStringWriteReader(
            process.getInputStream(), stdoutWrite, (int) workerContext.getStandardOutputLimit());
    ByteStringWriteReader stderrReader =
        new ByteStringWriteReader(
            process.getErrorStream(), stderrWrite, (int) workerContext.getStandardErrorLimit());

    Thread stdoutReaderThread = new Thread(stdoutReader, "Executor.stdoutReader");
    Thread stderrReaderThread = new Thread(stderrReader, "Executor.stderrReader");
    stdoutReaderThread.start();
    stderrReaderThread.start();

    Code statusCode = Code.OK;
    boolean processCompleted = false;
    try {
      if (timeout == null) {
        exitCode = process.waitFor();
        processCompleted = true;
      } else {
        long timeoutNanos = Durations.toNanos(timeout);
        long remainingNanoTime = timeoutNanos - (System.nanoTime() - startNanoTime);
        if (process.waitFor(remainingNanoTime, TimeUnit.NANOSECONDS)) {
          exitCode = process.exitValue();
          processCompleted = true;
        } else {
          log.log(
              Level.INFO,
              format(
                  "process timed out for %s after %ds",
                  operationName, Durations.toSeconds(timeout)));
          statusCode = Code.DEADLINE_EXCEEDED;
        }
      }
    } finally {
      if (!processCompleted) {
        process.destroy();
        int waitMillis = 1000;
        while (!process.waitFor(waitMillis, TimeUnit.MILLISECONDS)) {
          log.log(
              Level.INFO,
              format("process did not respond to termination for %s, killing it", operationName));
          process.destroyForcibly();
          waitMillis = 100;
        }
      }
    }

    // Now that the process is completed, extract the final stdout/stderr.
    ByteString stdout = ByteString.EMPTY;
    ByteString stderr = ByteString.EMPTY;
    try {
      stdoutReaderThread.join();
      stderrReaderThread.join();
      stdout = stdoutReader.getData();
      stderr = stderrReader.getData();

    } catch (Exception e) {
      log.log(Level.SEVERE, "error extracting stdout/stderr: ", e.getMessage());
    }

    resultBuilder.setExitCode(exitCode).setStdoutRaw(stdout).setStderrRaw(stderr);

    // allow debugging after an execution
    if (limits.debugAfterExecution) {
      // Obtain execution statistics recorded while the action executed.
      // Currently we can only source this data when using the sandbox.
      ExecutionStatistics executionStatistics = ExecutionStatistics.newBuilder().build();
      if (limits.useLinuxSandbox) {
        executionStatistics =
            ExecutionStatistics.newBuilder()
                .mergeFrom(
                    new FileInputStream(execDir.resolve("action_execution_statistics").toString()))
                .build();
      }

      return ExecutionDebugger.performAfterExecutionDebug(
          processBuilder, exitCode, limits, executionStatistics, resultBuilder);
    }

    return statusCode;
  }
}
