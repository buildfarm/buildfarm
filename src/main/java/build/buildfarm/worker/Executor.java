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
import java.nio.file.Path;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.TimeUnit;

class Executor implements Runnable {
  private final Worker worker;
  private final OperationContext operationContext;
  private final ExecuteActionStage owner;

  private static final OutputStream nullOutputStream = ByteStreams.nullOutputStream();

  Executor(Worker worker, OperationContext operationContext, ExecuteActionStage owner) {
    this.worker = worker;
    this.operationContext = operationContext;
    this.owner = owner;
  }

  private void runInterruptible() throws InterruptedException {
    Command command;
    try {
      command = Command.parseFrom(worker.instance.getBlob(operationContext.action.getCommandDigest()));
    } catch (InvalidProtocolBufferException ex) {
      owner.error().put(operationContext);
      owner.release();
      return;
    }

    ExecuteOperationMetadata executingMetadata = operationContext.metadata.toBuilder()
        .setStage(ExecuteOperationMetadata.Stage.EXECUTING)
        .build();

    Operation operation = operationContext.operation.toBuilder()
        .setMetadata(Any.pack(executingMetadata))
        .build();

    if (!worker.instance.putOperation(operation)) {
      owner.error().put(operationContext);
      owner.release();
      return;
    }

    final String operationName = operation.getName();
    Poller poller = new Poller(
        worker.config.getOperationPollPeriod(),
        () -> {
          boolean success = worker.instance.pollOperation(
              operationName,
              ExecuteOperationMetadata.Stage.EXECUTING);
          return success;
        });
    new Thread(poller).start();

    Duration timeout;
    if (operationContext.action.hasTimeout()) {
      timeout = operationContext.action.getTimeout();
    } else {
      timeout = null;
    }

    /* execute command */
    ActionResult.Builder resultBuilder;
    try {
      resultBuilder = executeCommand(
          operationContext.execDir,
          command,
          timeout,
          operationContext.metadata.getStdoutStreamName(),
          operationContext.metadata.getStderrStreamName());
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
              .setResult(resultBuilder.build())
              .build()))
          .build();
      owner.output().put(new OperationContext(
          operation,
          operationContext.execDir,
          operationContext.metadata,
          operationContext.action,
          operationContext.inputFiles,
          operationContext.inputDirectories));
    } else {
      owner.error().put(operationContext);
    }

    owner.release();

    worker.fileCache.update(operationContext.inputFiles, operationContext.inputDirectories);
  }

  @Override
  public void run() {
    try {
      runInterruptible();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private ActionResult.Builder executeCommand(
      Path execDir,
      Command command,
      Duration timeout,
      String stdoutStreamName,
      String stderrStreamName)
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

    if (stdoutStreamName != null && !stdoutStreamName.isEmpty() && worker.config.getStreamStdout()) {
      stdoutSink = worker.instance.getStreamOutput(stdoutStreamName);
    } else {
      stdoutSink = nullOutputStream;
    }
    if (stderrStreamName != null && !stderrStreamName.isEmpty() && worker.config.getStreamStderr()) {
      stderrSink = worker.instance.getStreamOutput(stderrStreamName);
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
      ActionResult.Builder resultBuilder = ActionResult.newBuilder()
          .setExitCode(exitValue);
      return resultBuilder;
    }

    ByteStringSinkReader stdoutReader = new ByteStringSinkReader(
        process.getInputStream(), stdoutSink);
    ByteStringSinkReader stderrReader = new ByteStringSinkReader(
        process.getErrorStream(), stderrSink);

    Thread stdoutReaderThread = new Thread(stdoutReader);
    Thread stderrReaderThread = new Thread(stderrReader);
    stdoutReaderThread.start();
    stderrReaderThread.start();

    boolean doneWaiting = false;
    if (timeout == null) {
      exitValue = process.waitFor();
    } else {
      while (!doneWaiting) {
        long timeoutNanos = timeout.getSeconds() * 1000000000L + timeout.getNanos();
        long remainingNanoTime = timeoutNanos - (System.nanoTime() - startNanoTime);
        if (remainingNanoTime > 0) {
          if (process.waitFor(remainingNanoTime, TimeUnit.NANOSECONDS)) {
            exitValue = process.exitValue();
            doneWaiting = true;
          }
        } else {
          process.destroyForcibly();
          process.waitFor(100, TimeUnit.MILLISECONDS); // fair trade, i think
          doneWaiting = true;
        }
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
    return ActionResult.newBuilder()
        .setExitCode(exitValue)
        .setStdoutRaw(stdoutReader.getData())
        .setStderrRaw(stderrReader.getData());
  }
}
