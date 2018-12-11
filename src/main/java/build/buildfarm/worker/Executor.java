// Copyright 2017 The Bazel Authors. All rights reserved.
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

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static build.buildfarm.v1test.ExecutionPolicy.PolicyCase.WRAPPER;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.logging.Level.SEVERE;

import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata.Stage;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.bazel.remote.execution.v2.Platform;
import build.bazel.remote.execution.v2.Platform.Property;
import build.buildfarm.v1test.ExecutingOperationMetadata;
import build.buildfarm.v1test.ExecutionPolicy;
import com.google.common.base.Stopwatch;
import com.google.common.io.ByteStreams;
import com.google.common.collect.ImmutableList;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Durations;
import com.google.rpc.Code;
import io.grpc.Deadline;
import java.nio.file.Path;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

class Executor implements Runnable {
  private static final int INCOMPLETE_EXIT_CODE = -1;
  private static final Logger logger = Logger.getLogger(Executor.class.getName());
  private static final OutputStream nullOutputStream = ByteStreams.nullOutputStream();

  private final WorkerContext workerContext;
  private final OperationContext operationContext;
  private final ExecuteActionStage owner;
  private int exitCode = INCOMPLETE_EXIT_CODE;

  Executor(WorkerContext workerContext, OperationContext operationContext, ExecuteActionStage owner) {
    this.workerContext = workerContext;
    this.operationContext = operationContext;
    this.owner = owner;
  }

  private long runInterruptible(Stopwatch stopwatch) throws InterruptedException {
    ExecuteOperationMetadata metadata;
    try {
      metadata = operationContext.operation
          .getMetadata().unpack(ExecuteOperationMetadata.class);
    } catch (InvalidProtocolBufferException e) {
      logger.log(SEVERE, "invalid execute operation metadata", e);
      return 0;
    }
    ExecuteOperationMetadata executingMetadata = metadata.toBuilder()
        .setStage(Stage.EXECUTING)
        .build();

    long startedAt = System.currentTimeMillis();

    Operation operation = operationContext.operation.toBuilder()
        .setMetadata(Any.pack(ExecutingOperationMetadata.newBuilder()
            .setStartedAt(startedAt)
            .setExecutingOn(workerContext.getName())
            .setExecuteOperationMetadata(executingMetadata)
            .setRequestMetadata(operationContext.queueEntry.getExecuteEntry().getRequestMetadata())
            .build()))
        .build();

    boolean operationUpdateSuccess = false;
    try {
      operationUpdateSuccess = workerContext.putOperation(operation, operationContext.action);
    } catch (IOException e) {
      logger.log(SEVERE, "error putting operation " + operation.getName(), e);
    }

    if (!operationUpdateSuccess) {
      logger.warning(
          String.format(
              "Executor::run(%s): could not transition to EXECUTING",
              operation.getName()));
      try {
        workerContext.destroyExecDir(operationContext.execDir);
      } catch (IOException destroyExecDirException) {
        logger.log(SEVERE, "error destroying exec dir " + operationContext.execDir.toString(), destroyExecDirException);
      }
      owner.error().put(operationContext);
      return 0;
    }

    Platform platform = operationContext.command.getPlatform();
    ImmutableList.Builder<ExecutionPolicy> policies = ImmutableList.builder();
    ExecutionPolicy defaultPolicy = workerContext.getExecutionPolicy("");
    if (defaultPolicy != null) {
      policies.add(defaultPolicy);
    }
    for (Property property : platform.getPropertiesList()) {
      if (property.getName().equals("execution-policy")) {
        policies.add(workerContext.getExecutionPolicy(property.getValue()));
      }
    }

    Duration timeout;
    if (operationContext.action.hasTimeout()) {
      timeout = operationContext.action.getTimeout();
    } else {
      timeout = null;
    }

    if (timeout == null && workerContext.hasDefaultActionTimeout()) {
      timeout = workerContext.getDefaultActionTimeout();
    }

    Deadline pollDeadline;
    if (timeout == null) {
      pollDeadline = Deadline.after(10, DAYS);
    } else {
      pollDeadline = Deadline.after(
          // 10s of padding for the timeout in question, so that we can guarantee cleanup
          (timeout.getSeconds() + 10) * 1000000 + timeout.getNanos() / 1000,
          MICROSECONDS);
    }

    final Thread executorThread = Thread.currentThread();
    Poller poller = workerContext.createPoller(
        "Executor",
        operationContext.queueEntry,
        Stage.EXECUTING,
        () -> executorThread.interrupt(),
        pollDeadline);

    /* execute command */
    workerContext.logInfo("Executor: Operation " + operation.getName() + " Executing command");

    long executeStartAt = System.nanoTime();

    ActionResult.Builder resultBuilder = ActionResult.newBuilder();
    Code statusCode;
    try {
      statusCode = executeCommand(
          operationContext.execDir,
          operationContext.command,
          timeout,
          "", // executingMetadata.getStdoutStreamName(),
          "", // executingMetadata.getStderrStreamName(),
          resultBuilder,
          policies.build());
    } catch (IOException e) {
      logger.log(SEVERE, "error executing command for " + operation.getName(), e);
      try {
        workerContext.destroyExecDir(operationContext.execDir);
      } catch (IOException destroyExecDirException) {
        logger.log(
            SEVERE,
            "error destroying exec dir " + operationContext.execDir.toString(),
            destroyExecDirException);
      }
      poller.stop();
      owner.error().put(operationContext);
      return 0;
    }

    Duration executedIn = Durations.fromNanos(System.nanoTime() - executeStartAt);

    logger.info(
        String.format(
            "Executor::executeCommand(%s): Completed command: exit code %d",
            operation.getName(),
            resultBuilder.getExitCode()));

    poller.stop();

    long executeUSecs = stopwatch.elapsed(MICROSECONDS);
    operation = operation.toBuilder()
        .setResponse(Any.pack(ExecuteResponse.newBuilder()
            .setResult(resultBuilder.build())
            .setStatus(com.google.rpc.Status.newBuilder()
                .setCode(statusCode.getNumber())
                .build())
            .build()))
        .build();
    OperationContext reportOperationContext = operationContext.toBuilder()
          .setOperation(operation)
          .setExecutedIn(executedIn)
          .build();
    if (owner.output().claim()) {
      try {
        owner.output().put(reportOperationContext);
      } catch (InterruptedException e) {
        owner.output().release();
        throw e;
      }
    } else {
      // FIXME we need to release the action root
      workerContext.logInfo("Executor: Operation " + operation.getName() + " Failed to claim output");

      owner.error().put(operationContext);
    }
    return stopwatch.elapsed(MICROSECONDS) - executeUSecs;
  }

  @Override
  public void run() {
    long stallUSecs = 0;
    Stopwatch stopwatch = Stopwatch.createStarted();
    try {
      stallUSecs = runInterruptible(stopwatch);
    } catch (InterruptedException e) {
      /* we can be interrupted when the poller fails */
      try {
        owner.error().put(operationContext);
      } catch (InterruptedException errorEx) {
        logger.log(SEVERE, "interrupted while erroring " + operationContext.operation.getName(), errorEx);
      }
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      logger.log(SEVERE, "errored while executing " + operationContext.operation.getName(), e);
      try {
        owner.error().put(operationContext);
      } catch (InterruptedException errorEx) {
        logger.log(SEVERE, "interrupted while erroring " + operationContext.operation.getName(), errorEx);
      }
      throw e;
    } finally {
      owner.releaseExecutor(
          operationContext.operation.getName(),
          stopwatch.elapsed(MICROSECONDS),
          stallUSecs,
          exitCode);
    }
  }

  private Code executeCommand(
      Path execDir,
      Command command,
      Duration timeout,
      String stdoutStreamName,
      String stderrStreamName,
      ActionResult.Builder resultBuilder,
      Iterable<ExecutionPolicy> policies)
      throws IOException, InterruptedException {
    ImmutableList.Builder<String> arguments = ImmutableList.builder();
    arguments.addAll(
        transform(
            filter(policies, (policy) -> policy.getPolicyCase() == WRAPPER),
            (policy) -> policy.getWrapper().getPath()));
    arguments.addAll(command.getArgumentsList());

    ProcessBuilder processBuilder =
        new ProcessBuilder(arguments.build())
            .directory(execDir.toAbsolutePath().toFile());

    Map<String, String> environment = processBuilder.environment();
    environment.clear();
    for (Command.EnvironmentVariable environmentVariable : command.getEnvironmentVariablesList()) {
      environment.put(environmentVariable.getName(), environmentVariable.getValue());
    }

    OutputStream stdoutSink = null, stderrSink = null;

    if (stdoutStreamName != null && !stdoutStreamName.isEmpty() && workerContext.getStreamStdout()) {
      stdoutSink = workerContext.getStreamOutput(stdoutStreamName);
    } else {
      stdoutSink = nullOutputStream;
    }
    if (stderrStreamName != null && !stderrStreamName.isEmpty() && workerContext.getStreamStderr()) {
      stderrSink = workerContext.getStreamOutput(stderrStreamName);
    } else {
      stderrSink = nullOutputStream;
    }

    long startNanoTime = System.nanoTime();
    Process process;
    try {
      synchronized (this) {
        process = processBuilder.start();
      }
      process.getOutputStream().close();
    } catch(IOException e) {
      logger.log(SEVERE, "could not start process for " + operationContext.operation.getName(), e);
      // again, should we do something else here??
      resultBuilder.setExitCode(INCOMPLETE_EXIT_CODE);
      return Code.INVALID_ARGUMENT;
    }

    ByteStringSinkReader stdoutReader = new ByteStringSinkReader(
        process.getInputStream(), stdoutSink);
    ByteStringSinkReader stderrReader = new ByteStringSinkReader(
        process.getErrorStream(), stderrSink);

    Thread stdoutReaderThread = new Thread(stdoutReader);
    Thread stderrReaderThread = new Thread(stderrReader);
    stdoutReaderThread.start();
    stderrReaderThread.start();

    Code statusCode = Code.OK;
    if (timeout == null) {
      exitCode = process.waitFor();
    } else {
      long timeoutNanos = timeout.getSeconds() * 1000000000L + timeout.getNanos();
      long remainingNanoTime = timeoutNanos - (System.nanoTime() - startNanoTime);
      if (process.waitFor(remainingNanoTime, TimeUnit.NANOSECONDS)) {
        exitCode = process.exitValue();
      } else {
        process.destroyForcibly();
        process.waitFor(100, TimeUnit.MILLISECONDS); // fair trade, i think
        statusCode = Code.DEADLINE_EXCEEDED;
      }
    }
    if (!stdoutReader.isComplete()) {
      stdoutReaderThread.interrupt();
    }
    stdoutReaderThread.join();
    if (!stderrReader.isComplete()) {
      stderrReaderThread.interrupt();
    }
    stderrReaderThread.join();
    resultBuilder
        .setExitCode(exitCode)
        .setStdoutRaw(stdoutReader.getData())
        .setStderrRaw(stderrReader.getData());
    return statusCode;
  }
}
