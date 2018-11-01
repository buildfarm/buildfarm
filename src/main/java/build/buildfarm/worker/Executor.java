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

import build.buildfarm.v1test.ExecutingOperationMetadata;
import com.google.common.io.ByteStreams;
import com.google.devtools.remoteexecution.v1test.ActionResult;
import com.google.devtools.remoteexecution.v1test.Command;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata;
import com.google.devtools.remoteexecution.v1test.ExecuteResponse;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Durations;
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
  private final PipelineStage owner;

  private static final OutputStream nullOutputStream = ByteStreams.nullOutputStream();

  Executor(WorkerContext workerContext, OperationContext operationContext, PipelineStage owner) {
    this.workerContext = workerContext;
    this.operationContext = operationContext;
    this.owner = owner;
  }

  private long runInterruptible() throws InterruptedException {
    ExecuteOperationMetadata executingMetadata = operationContext.metadata.toBuilder()
        .setStage(ExecuteOperationMetadata.Stage.EXECUTING)
        .build();

    long startedAt = System.currentTimeMillis();

    Operation operation = operationContext.operation.toBuilder()
        .setMetadata(Any.pack(ExecutingOperationMetadata.newBuilder()
            .setStartedAt(startedAt)
            .setExecutingOn(workerContext.getName())
            .setExecuteOperationMetadata(executingMetadata)
            .setRequestMetadata(operationContext.requestMetadata)
            .build()))
        .build();

    boolean operationUpdateSuccess = false;
    try {
      operationUpdateSuccess = workerContext.putOperation(operation, operationContext.action);
    } catch (IOException e) {
      e.printStackTrace();
    }

    if (!operationUpdateSuccess) {
      workerContext.logInfo("Executor: Operation " + operation.getName() + " is no longer valid");
      try {
        workerContext.destroyExecDir(operationContext.execDir);
      } catch (IOException destroyActionRootException) {
        destroyActionRootException.printStackTrace();
      }
      owner.error().put(operationContext);
      return 0;
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
    workerContext.logInfo("Executor: Operation " + operation.getName() + " Executing command");

    long executeStartAt = System.nanoTime();

    ActionResult.Builder resultBuilder = ActionResult.newBuilder();
    Code statusCode;
    try {
      statusCode = executeCommand(
          operationContext.execDir,
          operationContext.command,
          timeout,
          operationContext.metadata.getStdoutStreamName(),
          operationContext.metadata.getStderrStreamName(),
          resultBuilder);
    } catch (IOException e) {
      e.printStackTrace();
      try {
        workerContext.destroyExecDir(operationContext.execDir);
      } catch (IOException destroyActionRootException) {
        destroyActionRootException.printStackTrace();
      }
      poller.stop();
      owner.error().put(operationContext);
      return 0;
    }

    Duration executedIn = Durations.fromNanos(System.nanoTime() - executeStartAt);

    workerContext.logInfo("Executor: Operation " + operation.getName() + " Executed command: exit code " + resultBuilder.getExitCode());

    poller.stop();

    long waitStartTime = System.nanoTime();
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
    long waitTime = System.nanoTime() - waitStartTime;

    return waitTime;
  }

  @Override
  public void run() {
    long startTime = System.nanoTime();

    long waitTime;
    try {
      waitTime = runInterruptible();

      long endTime = System.nanoTime();
      workerContext.logInfo(String.format(
          "Executor::run(%s): %gms (%gms wait)",
          operationContext.operation.getName(),
          (endTime - startTime) / 1000000.0f,
          waitTime / 1000000.0f));
    } catch (InterruptedException e) {
      /* we can be interrupted when the poller fails */
      try {
        owner.error().put(operationContext);
      } catch (InterruptedException errorEx) {
        errorEx.printStackTrace();
      }
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      e.printStackTrace();
      try {
        owner.error().put(operationContext);
      } catch (InterruptedException errorEx) {
        errorEx.printStackTrace();
      }
      throw e;
    } finally {
      owner.release();
    }
  }

  private Code executeCommand(
      Path execDir,
      Command command,
      Duration timeout,
      String stdoutStreamName,
      String stderrStreamName,
      ActionResult.Builder resultBuilder)
      throws IOException, InterruptedException {
    ProcessBuilder processBuilder =
        new ProcessBuilder(command.getArgumentsList())
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
      synchronized (this) {
        process = processBuilder.start();
      }
      process.getOutputStream().close();
    } catch(IOException e) {
      e.printStackTrace();
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
