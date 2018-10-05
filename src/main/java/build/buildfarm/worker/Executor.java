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
import static com.google.common.collect.Lists.newArrayList;
import static build.buildfarm.v1test.ExecutionPolicy.PolicyCase.WRAPPER;

import build.buildfarm.v1test.ExecutionPolicy;
import com.google.common.io.ByteStreams;
import com.google.common.collect.ImmutableList;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.bazel.remote.execution.v2.Platform;
import build.bazel.remote.execution.v2.Platform.Property;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
import java.nio.file.Path;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.TimeUnit;

class Executor implements Runnable {
  private final WorkerContext workerContext;
  private final OperationContext operationContext;
  private final ExecuteActionStage owner;

  private static final OutputStream nullOutputStream = ByteStreams.nullOutputStream();

  Executor(WorkerContext workerContext, OperationContext operationContext, ExecuteActionStage owner) {
    this.workerContext = workerContext;
    this.operationContext = operationContext;
    this.owner = owner;
  }

  private void runInterruptible() throws InterruptedException {
    ExecuteOperationMetadata executingMetadata = operationContext.metadata.toBuilder()
        .setStage(ExecuteOperationMetadata.Stage.EXECUTING)
        .build();

    Operation operation = operationContext.operation.toBuilder()
        .setMetadata(Any.pack(executingMetadata))
        .build();

    if (!workerContext.putOperation(operation)) {
      owner.error().put(operationContext);
      owner.release();
      return;
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

    final Thread executorThread = Thread.currentThread();
    Poller poller = workerContext.createPoller(
        "Executor",
        operation.getName(),
        ExecuteOperationMetadata.Stage.EXECUTING,
        () -> executorThread.interrupt());

    Duration timeout;
    if (operationContext.action.hasTimeout()) {
      timeout = operationContext.action.getTimeout();
    } else {
      timeout = null;
    }

    if (timeout == null && workerContext.hasDefaultActionTimeout()) {
      timeout = workerContext.getDefaultActionTimeout();
    }

    /* execute command */
    ActionResult.Builder resultBuilder = ActionResult.newBuilder();
    Code statusCode;
    try {
      statusCode = executeCommand(
          operationContext.execDir,
          operationContext.command,
          timeout,
          operationContext.metadata.getStdoutStreamName(),
          operationContext.metadata.getStderrStreamName(),
          resultBuilder,
          policies.build());
    } catch (IOException ex) {
      poller.stop();
      owner.error().put(operationContext);
      owner.release();
      return;
    }

    poller.stop();

    if (owner.output().claim()) {
      operation = operation.toBuilder()
          .setResponse(Any.pack(ExecuteResponse.newBuilder()
              .setResult(resultBuilder)
              .setStatus(com.google.rpc.Status.newBuilder()
                  .setCode(statusCode.getNumber()))
              .build()))
          .build();
      owner.output().put(new OperationContext(
          operation,
          operationContext.execDir,
          operationContext.metadata,
          operationContext.action,
          operationContext.command));
    } else {
      owner.error().put(operationContext);
    }

    owner.release();
  }

  @Override
  public void run() {
    try {
      runInterruptible();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
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
    int exitValue = -1;
    Process process;
    try {
      process = processBuilder.start();
      process.getOutputStream().close();
    } catch(IOException ex) {
      ex.printStackTrace();
      // again, should we do something else here??
      resultBuilder.setExitCode(exitValue);
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
      exitValue = process.waitFor();
    } else {
      long timeoutNanos = timeout.getSeconds() * 1000000000L + timeout.getNanos();
      long remainingNanoTime = timeoutNanos - (System.nanoTime() - startNanoTime);
      if (process.waitFor(remainingNanoTime, TimeUnit.NANOSECONDS)) {
        exitValue = process.exitValue();
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
        .setExitCode(exitValue)
        .setStdoutRaw(stdoutReader.getData())
        .setStderrRaw(stderrReader.getData());
    return statusCode;
  }
}
