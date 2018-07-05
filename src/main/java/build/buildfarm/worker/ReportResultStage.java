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

import build.buildfarm.common.DigestUtil;
import build.buildfarm.instance.stub.Chunker;
import build.buildfarm.v1test.CASInsertionPolicy;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.OutputDirectory;
import build.bazel.remote.execution.v2.OutputFile;
import build.bazel.remote.execution.v2.Tree;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Status;
import com.google.rpc.Code;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.function.Consumer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;

public class ReportResultStage extends PipelineStage {
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
    public void put(OperationContext operation) { }
    @Override
    public void setInput(PipelineStage input) { }
    @Override
    public void run() { }
    @Override
    public void close() { }
    @Override
    public boolean isClosed() { return false; }
  }

  public ReportResultStage(WorkerContext workerContext, PipelineStage error) {
    super(workerContext, new NullStage(), error);
    queue = new ArrayBlockingQueue<>(1);
  }

  @Override
  public OperationContext take() throws InterruptedException {
    return queue.take();
  }

  @Override
  public void put(OperationContext operationContext) throws InterruptedException {
    queue.put(operationContext);
  }

  private DigestUtil getDigestUtil() {
    return workerContext.getDigestUtil();
  }

  @VisibleForTesting
  public void uploadOutputs(
      ActionResult.Builder result,
      Path execRoot,
      Collection<String> outputFiles,
      Collection<String> outputDirs)
      throws IOException, InterruptedException {
    UploadManifest manifest = new UploadManifest(
        getDigestUtil(),
        result,
        execRoot,
        /* allowSymlinks= */ true,
        workerContext.getInlineContentLimit());

    manifest.addFiles(
        Iterables.transform(outputFiles, (file) -> execRoot.resolve(file)),
        workerContext.getFileCasPolicy());
    manifest.addDirectories(
        Iterables.transform(outputDirs, (dir) -> execRoot.resolve(dir)));

    /* put together our outputs and update the result */
    if (result.getStdoutRaw().size() > 0) {
      manifest.addContent(
          result.getStdoutRaw(),
          workerContext.getStdoutCasPolicy(),
          result::setStdoutRaw,
          result::setStdoutDigest);
    }
    if (result.getStderrRaw().size() > 0) {
      manifest.addContent(
          result.getStderrRaw(),
          workerContext.getStderrCasPolicy(),
          result::setStderrRaw,
          result::setStderrDigest);
    }

    List<Chunker> filesToUpload = new ArrayList<>();

    Map<Digest, Path> digestToFile = manifest.getDigestToFile();
    Map<Digest, Chunker> digestToChunkers = manifest.getDigestToChunkers();
    Collection<Digest> digests = new ArrayList<>();
    digests.addAll(digestToFile.keySet());
    digests.addAll(digestToChunkers.keySet());

    for (Digest digest : digests) {
      Chunker chunker;
      Path file = digestToFile.get(digest);
      if (file != null) {
        chunker = new Chunker(file, digest);
      } else {
        chunker = digestToChunkers.get(digest);
      }
      if (chunker != null) {
        filesToUpload.add(chunker);
      }
    }

    if (!filesToUpload.isEmpty()) {
      workerContext.getUploader().uploadBlobs(filesToUpload);
    }
  }

  @Override
  protected OperationContext tick(OperationContext operationContext) throws InterruptedException {
    final String operationName = operationContext.operation.getName();
    Poller poller = workerContext.createPoller(
        "ReportResultStage",
        operationContext.operation.getName(),
        ExecuteOperationMetadata.Stage.EXECUTING,
        () -> {});

    ExecuteResponse executeResponse;
    try {
      executeResponse = operationContext.operation
          .getResponse().unpack(ExecuteResponse.class);
    } catch (InvalidProtocolBufferException ex) {
      poller.stop();
      return null;
    }

    ActionResult.Builder resultBuilder = executeResponse.getResult().toBuilder();
    Status.Builder status = executeResponse.getStatus().toBuilder();
    try {
      uploadOutputs(
          resultBuilder,
          operationContext.execDir,
          operationContext.command.getOutputFilesList(),
          operationContext.command.getOutputDirectoriesList());
    } catch (IllegalStateException e) {
      status
          .setCode(Code.FAILED_PRECONDITION.getNumber())
          .setMessage(e.getMessage());
    } catch (IOException ex) {
      poller.stop();
      return null;
    }

    ActionResult result = resultBuilder.build();
    if (!operationContext.action.getDoNotCache() && resultBuilder.getExitCode() == 0) {
      workerContext.putActionResult(DigestUtil.asActionKey(operationContext.metadata.getActionDigest()), result);
    }

    ExecuteOperationMetadata metadata = operationContext.metadata.toBuilder()
        .setStage(ExecuteOperationMetadata.Stage.COMPLETED)
        .build();

    Operation operation = operationContext.operation.toBuilder()
        .setDone(true)
        .setMetadata(Any.pack(metadata))
        .setResponse(Any.pack(executeResponse.toBuilder()
            .setResult(result)
            .setStatus(status)
            .build()))
        .build();

    poller.stop();

    if (!workerContext.putOperation(operation)) {
      return null;
    }

    return new OperationContext(
        operation,
        operationContext.execDir,
        metadata,
        operationContext.action,
        operationContext.command);
  }

  @Override
  protected void after(OperationContext operationContext) {
    try {
      workerContext.removeDirectory(operationContext.execDir);
    } catch (IOException ex) {
    }
  }
}
