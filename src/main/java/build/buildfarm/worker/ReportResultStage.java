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

import build.buildfarm.common.Digests;
import build.buildfarm.instance.Instance;
import build.buildfarm.v1test.CASInsertionControl;
import com.google.common.collect.ImmutableList;
import com.google.devtools.remoteexecution.v1test.Action;
import com.google.devtools.remoteexecution.v1test.ActionResult;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata;
import com.google.devtools.remoteexecution.v1test.ExecuteResponse;
import com.google.devtools.remoteexecution.v1test.OutputFile;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;

class ReportResultStage extends PipelineStage {
  private final BlockingQueue<OperationContext> queue;

  public static class NullStage extends PipelineStage {
    public NullStage() {
      super(null, null, null);
    }

    @Override
    public boolean claim() { return true; }
    @Override
    public void release() { }
    @Override
    public OperationContext take() { throw new UnsupportedOperationException(); }
    @Override
    public void offer(OperationContext operation) { }
    @Override
    public void setInput(PipelineStage input) { }
    @Override
    public void run() { }
    @Override
    public void close() { }
    @Override
    public boolean isClosed() { return false; }
  }

  ReportResultStage(Worker worker, PipelineStage error) {
    super(worker, new NullStage(), error);
    queue = new ArrayBlockingQueue<>(1);
  }

  @Override
  public OperationContext take() throws InterruptedException {
    return queue.take();
  }

  @Override
  public void offer(OperationContext operationContext) {
    queue.offer(operationContext);
  }

  private void updateActionResultStdOutputs(
      ActionResult.Builder resultBuilder,
      ImmutableList.Builder<ByteString> contents) {
    ByteString stdoutRaw = resultBuilder.getStdoutRaw();
    if (stdoutRaw.size() > 0) {
      CASInsertionControl control = worker.config.getStdoutCasControl();
      boolean withinLimit = stdoutRaw.size() <= control.getLimit();
      if (!withinLimit) {
        resultBuilder.setStdoutRaw(ByteString.EMPTY);
      }
      if (control.getPolicy() == CASInsertionControl.Policy.ALWAYS_INSERT ||
          (!withinLimit && control.getPolicy() == CASInsertionControl.Policy.INSERT_ABOVE_LIMIT)) {
        resultBuilder.setStdoutDigest(Digests.computeDigest(stdoutRaw));
      }
    }

    ByteString stderrRaw = resultBuilder.getStderrRaw();
    if (stderrRaw.size() > 0) {
      CASInsertionControl control = worker.config.getStderrCasControl();
      boolean withinLimit = stderrRaw.size() <= control.getLimit();
      if (!withinLimit) {
        resultBuilder.setStderrRaw(ByteString.EMPTY);
      }
      if (control.getPolicy() == CASInsertionControl.Policy.ALWAYS_INSERT ||
          (!withinLimit && control.getPolicy() == CASInsertionControl.Policy.INSERT_ABOVE_LIMIT)) {
        contents.add(stderrRaw);
        resultBuilder.setStderrDigest(Digests.computeDigest(stderrRaw));
      }
    }
  }

  @Override
  protected OperationContext tick(OperationContext operationContext) {
    final String operationName = operationContext.operation.getName();
    Poller poller = new Poller(worker.config.getOperationPollPeriod(), () -> {
          boolean success = worker.instance.pollOperation(
              operationName,
              ExecuteOperationMetadata.Stage.EXECUTING);
          return success;
        });
    new Thread(poller).start();

    ActionResult.Builder resultBuilder;
    try {
      resultBuilder = operationContext
          .operation.getResponse().unpack(ExecuteResponse.class).getResult().toBuilder();
    } catch (InvalidProtocolBufferException ex) {
      poller.stop();
      return null;
    }

    ImmutableList.Builder<ByteString> contents = new ImmutableList.Builder<>();
    CASInsertionControl control = worker.config.getFileCasControl();
    for (String outputFile : operationContext.action.getOutputFilesList()) {
      Path outputPath = operationContext.execDir.resolve(outputFile);
      if (!Files.exists(outputPath)) {
        continue;
      }

      // FIXME put the output into the fileCache
      // FIXME this needs to be streamed to the server, not read to completion, but
      // this is a constraint of not knowing the hash, however, if we put the entry
      // into the cache, we can likely do so, stream the output up, and be done
      //
      // will run into issues if we end up blocking on the cache insertion, might
      // want to decrement input references *before* this to ensure that we cannot
      // cause an internal deadlock

      ByteString content;
      try {
        InputStream inputStream = Files.newInputStream(outputPath);
        content = ByteString.readFrom(inputStream);
        inputStream.close();
      } catch (IOException ex) {
        continue;
      }
      OutputFile.Builder outputFileBuilder = resultBuilder.addOutputFilesBuilder()
          .setPath(outputFile)
          .setIsExecutable(Files.isExecutable(outputPath));
      boolean withinLimit = content.size() <= worker.config.getFileCasControl().getLimit();
      if (withinLimit) {
        outputFileBuilder.setContent(content);
      }
      if (control.getPolicy() == CASInsertionControl.Policy.ALWAYS_INSERT ||
          (!withinLimit && control.getPolicy() == CASInsertionControl.Policy.INSERT_ABOVE_LIMIT)) {
        contents.add(content);

        // FIXME make this happen with putAllBlobs
        Digest outputDigest = Digests.computeDigest(content);
        outputFileBuilder.setDigest(outputDigest);
      }
    }

    /* put together our outputs and update the result */
    updateActionResultStdOutputs(resultBuilder, contents);

    try {
      worker.instance.putAllBlobs(contents.build());
    } catch (IOException ex) {
    } catch (InterruptedException ex) {
      poller.stop();
      return null;
    }

    ActionResult result = resultBuilder.build();
    if (!operationContext.action.getDoNotCache() && resultBuilder.getExitCode() == 0) {
      worker.instance.putActionResult(operationContext.metadata.getActionDigest(), result);
    }

    ExecuteOperationMetadata metadata = operationContext.metadata.toBuilder()
        .setStage(ExecuteOperationMetadata.Stage.COMPLETED)
        .build();

    Operation operation = operationContext.operation.toBuilder()
        .setDone(true)
        .setMetadata(Any.pack(metadata))
        .setResponse(Any.pack(ExecuteResponse.newBuilder()
            .setResult(result)
            .setCachedResult(false)
            .build()))
        .build();

    poller.stop();

    if (!worker.instance.putOperation(operation)) {
      return null;
    }

    return new OperationContext(
        operation,
        operationContext.execDir,
        metadata,
        operationContext.action,
        operationContext.inputFiles);
  }

  @Override
  protected void after(OperationContext operationContext) {
    try {
      Worker.removeDirectory(operationContext.execDir);
    } catch (IOException ex) {
    }
  }
}
