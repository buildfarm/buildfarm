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
import build.buildfarm.instance.Instance;
import build.buildfarm.v1test.CASInsertionPolicy;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.devtools.remoteexecution.v1test.Action;
import com.google.devtools.remoteexecution.v1test.ActionResult;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.Directory;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata;
import com.google.devtools.remoteexecution.v1test.ExecuteResponse;
import com.google.devtools.remoteexecution.v1test.FileNode;
import com.google.devtools.remoteexecution.v1test.OutputDirectory;
import com.google.devtools.remoteexecution.v1test.OutputFile;
import com.google.devtools.remoteexecution.v1test.Tree;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
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

  private static int inlineOrDigest(
      ByteString content,
      CASInsertionPolicy policy,
      ImmutableList.Builder<ByteString> contents,
      int inlineContentBytes,
      int inlineContentLimit,
      Runnable setInline,
      Consumer<ByteString> setDigest) {
    boolean withinLimit = inlineContentBytes + content.size() <= inlineContentLimit;
    if (withinLimit) {
      setInline.run();
      inlineContentBytes += content.size();
    }
    if (policy.equals(CASInsertionPolicy.ALWAYS_INSERT) ||
        (!withinLimit && policy.equals(CASInsertionPolicy.INSERT_ABOVE_LIMIT))) {
      contents.add(content);
      setDigest.accept(content);
    }
    return inlineContentBytes;
  }

  private int updateActionResultStdOutputs(
      ActionResult.Builder resultBuilder,
      ImmutableList.Builder<ByteString> contents,
      int inlineContentBytes) {
    ByteString stdoutRaw = resultBuilder.getStdoutRaw();
    if (stdoutRaw.size() > 0) {
      // reset to allow policy to determine inlining
      resultBuilder.setStdoutRaw(ByteString.EMPTY);
      inlineContentBytes = inlineOrDigest(
          stdoutRaw,
          workerContext.getStdoutCasPolicy(),
          contents,
          inlineContentBytes,
          workerContext.getInlineContentLimit(),
          () -> resultBuilder.setStdoutRaw(stdoutRaw),
          (content) -> resultBuilder.setStdoutDigest(getDigestUtil().compute(content)));
    }

    ByteString stderrRaw = resultBuilder.getStderrRaw();
    if (stderrRaw.size() > 0) {
      // reset to allow policy to determine inlining
      resultBuilder.setStderrRaw(ByteString.EMPTY);
      inlineContentBytes = inlineOrDigest(
          stderrRaw,
          workerContext.getStderrCasPolicy(),
          contents,
          inlineContentBytes,
          workerContext.getInlineContentLimit(),
          () -> resultBuilder.setStderrRaw(stdoutRaw),
          (content) -> resultBuilder.setStderrDigest(getDigestUtil().compute(content)));
    }

    return inlineContentBytes;
  }

  @VisibleForTesting
  public void uploadOutputs(
      ActionResult.Builder resultBuilder,
      Path root,
      Iterable<String> outputFiles,
      Iterable<String> outputDirs)
      throws IOException, InterruptedException {
    int inlineContentBytes = 0;
    ImmutableList.Builder<ByteString> contents = new ImmutableList.Builder<>();
    for (String outputFile : outputFiles) {
      Path outputPath = root.resolve(outputFile);
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
      inlineContentBytes = inlineOrDigest(
          content,
          workerContext.getFileCasPolicy(),
          contents,
          inlineContentBytes,
          workerContext.getInlineContentLimit(),
          () -> outputFileBuilder.setContent(content),
          (fileContent) -> outputFileBuilder.setDigest(getDigestUtil().compute(fileContent)));
    }

    for (String outputDir : outputDirs) {
      Path outputDirPath = root.resolve(outputDir);
      if (!Files.exists(outputDirPath)) {
        continue;
      }

      Tree.Builder treeBuilder = Tree.newBuilder();
      Directory.Builder outputRoot = treeBuilder.getRootBuilder();
      Files.walkFileTree(outputDirPath, new SimpleFileVisitor<Path>() {
        Directory.Builder currentDirectory = null;
        Stack<Directory.Builder> path = new Stack<>();

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          ByteString content;
          try (InputStream inputStream = Files.newInputStream(file)) {
            content = ByteString.readFrom(inputStream);
          } catch (IOException e) {
            e.printStackTrace();
            return FileVisitResult.CONTINUE;
          }

          // should we cast to PosixFilePermissions and do gymnastics there for executable?

          // TODO symlink per revision proposal
          contents.add(content);
          FileNode.Builder fileNodeBuilder = currentDirectory.addFilesBuilder()
              .setName(file.getFileName().toString())
              .setDigest(getDigestUtil().compute(content))
              .setIsExecutable(Files.isExecutable(file));
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
          path.push(currentDirectory);
          if (dir.equals(outputDirPath)) {
            currentDirectory = outputRoot;
          } else {
            currentDirectory = treeBuilder.addChildrenBuilder();
          }
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
          Directory.Builder parentDirectory = path.pop();
          if (parentDirectory != null) {
            parentDirectory.addDirectoriesBuilder()
                .setName(dir.getFileName().toString())
                .setDigest(getDigestUtil().compute(currentDirectory.build()));
          }
          currentDirectory = parentDirectory;
          return FileVisitResult.CONTINUE;
        }
      });
      Tree tree = treeBuilder.build();
      ByteString treeBlob = tree.toByteString();
      contents.add(treeBlob);
      Digest treeDigest = getDigestUtil().compute(treeBlob);
      resultBuilder.addOutputDirectoriesBuilder()
          .setPath(outputDir)
          .setTreeDigest(treeDigest);
    }

    /* put together our outputs and update the result */
    updateActionResultStdOutputs(resultBuilder, contents, inlineContentBytes);

    List<ByteString> blobs = contents.build();
    if (!blobs.isEmpty()) {
      workerContext.putAllBlobs(contents.build());
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

    ActionResult.Builder resultBuilder;
    try {
      resultBuilder = operationContext
          .operation.getResponse().unpack(ExecuteResponse.class).getResult().toBuilder();
    } catch (InvalidProtocolBufferException ex) {
      poller.stop();
      return null;
    }

    try {
      uploadOutputs(
          resultBuilder,
          operationContext.execDir,
          operationContext.action.getOutputFilesList(),
          operationContext.action.getOutputDirectoriesList());
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
        .setResponse(Any.pack(ExecuteResponse.newBuilder()
            .setResult(result)
            .setCachedResult(false)
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
        operationContext.action);
  }

  @Override
  protected void after(OperationContext operationContext) {
    try {
      workerContext.removeDirectory(operationContext.execDir);
    } catch (IOException ex) {
    }
  }
}
