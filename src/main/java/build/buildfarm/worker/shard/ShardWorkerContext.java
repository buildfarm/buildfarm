// Copyright 2018 The Bazel Authors. All rights reserved.
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

package build.buildfarm.worker.shard;

import static java.util.logging.Level.SEVERE;
import static java.util.concurrent.TimeUnit.DAYS;

import build.buildfarm.common.ContentAddressableStorage.Blob;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.Instance.MatchListener;
import build.buildfarm.instance.stub.Retrier;
import build.buildfarm.instance.stub.Retrier.Backoff;
import build.buildfarm.worker.Poller;
import build.buildfarm.worker.WorkerContext;
import build.buildfarm.v1test.CASInsertionPolicy;
import build.buildfarm.v1test.QueuedOperationMetadata;
import com.google.common.collect.ImmutableList;
import com.google.devtools.remoteexecution.v1test.Action;
import com.google.devtools.remoteexecution.v1test.ActionResult;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.Directory;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata.Stage;
import com.google.devtools.remoteexecution.v1test.FileNode;
import com.google.devtools.remoteexecution.v1test.OutputFile;
import com.google.devtools.remoteexecution.v1test.Platform;
import com.google.devtools.remoteexecution.v1test.Tree;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Deadline;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Consumer;
import java.util.logging.Logger;

class ShardWorkerContext implements WorkerContext {
  private static final Logger logger = Logger.getLogger(ShardWorkerContext.class.getName());

  private final String name;
  private final Platform platform;
  private final Duration operationPollPeriod;
  private final OperationPoller operationPoller;
  private final int inlineContentLimit;
  private final int executeStageWidth;
  private final ExecFileSystem execFileSystem;
  private final Instance instance;
  private final Set<String> activeOperations = new ConcurrentSkipListSet<>();

  ShardWorkerContext(
      String name,
      Platform platform,
      Duration operationPollPeriod,
      OperationPoller operationPoller,
      int inlineContentLimit,
      int executeStageWidth,
      ExecFileSystem execFileSystem,
      Instance instance) {
    this.name = name;
    this.operationPollPeriod = operationPollPeriod;
    this.operationPoller = operationPoller;
    this.inlineContentLimit = inlineContentLimit;
    this.executeStageWidth = executeStageWidth;
    this.execFileSystem = execFileSystem;
    this.instance = instance;
    this.platform = platform;
  }

  private static Retrier createBackplaneRetrier() {
    return new Retrier(
        Backoff.exponential(
              java.time.Duration.ofMillis(/*options.experimentalRemoteRetryStartDelayMillis=*/ 100),
              java.time.Duration.ofMillis(/*options.experimentalRemoteRetryMaxDelayMillis=*/ 5000),
              /*options.experimentalRemoteRetryMultiplier=*/ 2,
              /*options.experimentalRemoteRetryJitter=*/ 0.1,
              /*options.experimentalRemoteRetryMaxAttempts=*/ 5),
          Retrier.REDIS_IS_RETRIABLE);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Poller createPoller(String name, String operationName, Stage stage) {
    return createPoller(name, operationName, stage, () -> {}, Deadline.after(10, DAYS));
  }

  @Override
  public Poller createPoller(String name, String operationName, Stage stage, Runnable onFailure, Deadline deadline) {
    Poller poller = new Poller(
        operationPollPeriod,
        () -> {
          boolean success = false;
          try {
            success = operationPoller.poll(operationName, stage, System.currentTimeMillis() + 30 * 1000);
          } catch (IOException e) {
            logger.log(SEVERE, "error polling " + operationName, e);
          }

          logInfo(name + ": poller: Completed Poll for " + operationName + ": " + (success ? "OK" : "Failed"));
          if (!success) {
            onFailure.run();
          }
          return success;
        },
        () -> {
          logInfo(name + ": poller: Deadline expired for " + operationName);
          onFailure.run();
        },
        deadline);
    new Thread(poller).start();
    return poller;
  }

  @Override
  public DigestUtil getDigestUtil() {
    return instance.getDigestUtil();
  }

  @Override
  public void match(MatchListener listener) throws InterruptedException {
    // instance is a horrible place for this method's implementation for shard
    instance.match(platform, new MatchListener() {
      @Override
      public void onWaitStart() {
        listener.onWaitStart();
      }

      @Override
      public void onWaitEnd() {
        listener.onWaitEnd();
      }

      @Override
      public boolean onOperationName(String operationName) {
        if (activeOperations.contains(operationName)) {
          logger.severe("WorkerContext::match: WARNING matched duplicate operation " + operationName);
          return listener.onOperation(null);
        }
        activeOperations.add(operationName);
        boolean success = listener.onOperationName(operationName);
        if (!success) {
          try {
            // fast path to requeue, implementation specific
            operationPoller.poll(operationName, Stage.QUEUED, 0);
          } catch (IOException e) {
            logger.log(SEVERE, "Failure while trying to fast requeue " + operationName, e);
          }
        }
        return success;
      }

      @Override
      public boolean onOperation(Operation operation) {
        // could not fetch
        if (operation.getDone()) {
          activeOperations.remove(operation.getName());
          listener.onOperation(null);
          try {
            operationPoller.poll(operation.getName(), Stage.QUEUED, 0);
          } catch (IOException e) {
            logger.log(SEVERE, "Failure while trying to fast requeue " + operation.getName(), e);
          }
          return false;
        }

        boolean success = listener.onOperation(operation);
        if (!success) {
          try {
            requeue(operation);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
          }
        }
        return success;
      }
    });
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
  }

  private ExecuteOperationMetadata expectExecuteOperationMetadata(Operation operation) {
    Any metadata = operation.getMetadata();
    if (metadata == null) {
      return null;
    }

    if (metadata.is(QueuedOperationMetadata.class)) {
      try {
        return operation.getMetadata().unpack(QueuedOperationMetadata.class).getExecuteOperationMetadata();
      } catch(InvalidProtocolBufferException e) {
        logger.log(SEVERE, "invalid queued operation metadata", e);
        return null;
      }
    }

    if (metadata.is(ExecuteOperationMetadata.class)) {
      try {
        return operation.getMetadata().unpack(ExecuteOperationMetadata.class);
      } catch(InvalidProtocolBufferException e) {
        logger.log(SEVERE, "invalid execute operation metadata", e);
        return null;
      }
    }

    return null;
  }

  @Override
  public void requeue(Operation operation) throws InterruptedException {
    deactivate(operation);
    try {
      operationPoller.poll(operation.getName(), Stage.QUEUED, 0);
    } catch (IOException e) {
      // ignore, at least dispatcher will pick us up in 30s
      logger.log(SEVERE, "error polling operation: " + operation.getName(), e);
    }
  }

  @Override
  public void deactivate(Operation operation) {
    activeOperations.remove(operation.getName());
  }

  @Override
  public void logInfo(String msg) {
    logger.info(msg);
  }

  @Override
  public CASInsertionPolicy getFileCasPolicy() {
    return CASInsertionPolicy.ALWAYS_INSERT;
  }

  @Override
  public CASInsertionPolicy getStdoutCasPolicy() {
    return CASInsertionPolicy.ALWAYS_INSERT;
  }

  @Override
  public CASInsertionPolicy getStderrCasPolicy() {
    return CASInsertionPolicy.ALWAYS_INSERT;
  }

  @Override
  public int getExecuteStageWidth() {
    return executeStageWidth;
  }

  @Override
  public int getTreePageSize() {
    return 0;
  }

  @Override
  public boolean hasDefaultActionTimeout() {
    return false;
  }

  @Override
  public boolean hasMaximumActionTimeout() {
    return false;
  }

  @Override
  public boolean getStreamStdout() {
    return true;
  }

  @Override
  public boolean getStreamStderr() {
    return true;
  }

  @Override
  public Duration getDefaultActionTimeout() {
    return null;
  }

  @Override
  public Duration getMaximumActionTimeout() {
    return null;
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
          getStdoutCasPolicy(),
          contents,
          inlineContentBytes,
          inlineContentLimit,
          () -> resultBuilder.setStdoutRaw(stdoutRaw),
          (content) -> resultBuilder.setStdoutDigest(getDigestUtil().compute(content)));
    }

    ByteString stderrRaw = resultBuilder.getStderrRaw();
    if (stderrRaw.size() > 0) {
      // reset to allow policy to determine inlining
      resultBuilder.setStderrRaw(ByteString.EMPTY);
      inlineContentBytes = inlineOrDigest(
          stderrRaw,
          getStderrCasPolicy(),
          contents,
          inlineContentBytes,
          inlineContentLimit,
          () -> resultBuilder.setStderrRaw(stdoutRaw),
          (content) -> resultBuilder.setStderrDigest(getDigestUtil().compute(content)));
    }

    return inlineContentBytes;
  }

  private void putAllBlobs(Iterable<ByteString> blobs) {
    for (ByteString content : blobs) {
      if (content.size() > 0) {
        Blob blob = new Blob(content, instance.getDigestUtil());
        execFileSystem.getStorage().put(blob);
      }
    }
  }

  @Override
  public void uploadOutputs(
      ActionResult.Builder resultBuilder,
      Path actionRoot,
      Iterable<String> outputFiles,
      Iterable<String> outputDirs)
      throws IOException, InterruptedException {
    int inlineContentBytes = 0;
    ImmutableList.Builder<ByteString> contents = new ImmutableList.Builder<>();
    for (String outputFile : outputFiles) {
      Path outputPath = actionRoot.resolve(outputFile);
      if (!Files.exists(outputPath)) {
        logInfo("ReportResultStage: " + outputPath + " does not exist...");
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
      try (InputStream inputStream = Files.newInputStream(outputPath)) {
        content = ByteString.readFrom(inputStream);
      } catch (IOException e) {
        continue;
      }

      OutputFile.Builder outputFileBuilder = resultBuilder.addOutputFilesBuilder()
          .setPath(outputFile)
          .setIsExecutable(Files.isExecutable(outputPath));
      inlineContentBytes = inlineOrDigest(
          content,
          getFileCasPolicy(),
          contents,
          inlineContentBytes,
          inlineContentLimit,
          () -> outputFileBuilder.setContent(content),
          (fileContent) -> outputFileBuilder.setDigest(getDigestUtil().compute(fileContent)));
    }

    for (String outputDir : outputDirs) {
      Path outputDirPath = actionRoot.resolve(outputDir);
      if (!Files.exists(outputDirPath)) {
        logInfo("ReportResultStage: " + outputDir + " does not exist...");
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
            logger.log(SEVERE, "error loading content from " + file.toString(), e);
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
      putAllBlobs(blobs);
    }
  }

  private void logComplete(String operationName) {
    logger.info("CompletedOperation: " + operationName);
  }

  @Override
  public boolean putOperation(Operation operation, Action action) throws IOException, InterruptedException {
    boolean success = createBackplaneRetrier().execute(() -> instance.putOperation(operation));
    if (success && operation.getDone()) {
      logComplete(operation.getName());
    }
    return success;
  }

  @Override
  public Path createExecDir(String operationName, Map<Digest, Directory> directoriesIndex, Action action) throws IOException, InterruptedException {
    return execFileSystem.createExecDir(operationName, directoriesIndex, action);
  }

  // might want to split for removeDirectory and decrement references to avoid removing for streamed output
  @Override
  public void destroyExecDir(Path execDir) throws IOException, InterruptedException {
    execFileSystem.destroyExecDir(execDir);
  }

  @Override
  public void putActionResult(ActionKey actionKey, ActionResult actionResult) throws IOException, InterruptedException {
    createBackplaneRetrier().execute(() -> {
      instance.putActionResult(actionKey, actionResult);
      return null;
    });
  }

  @Override
  public OutputStream getStreamOutput(String name) {
    throw new UnsupportedOperationException();
  }
}
