// Copyright 2020 The Bazel Authors. All rights reserved.
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

package build.buildfarm.worker.operationqueue;

import static build.buildfarm.common.io.Utils.getInterruptiblyOrIOException;
import static build.buildfarm.instance.Utils.getBlob;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.WARNING;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.ExecutionStage;
import build.buildfarm.cas.cfc.CASFileCache;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.Poller;
import build.buildfarm.common.Size;
import build.buildfarm.common.Write;
import build.buildfarm.common.grpc.Retrier;
import build.buildfarm.common.io.Directories;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.Instance.MatchListener;
import build.buildfarm.instance.stub.ByteStreamUploader;
import build.buildfarm.instance.stub.Chunker;
import build.buildfarm.v1test.CASInsertionPolicy;
import build.buildfarm.v1test.ExecutionPolicy;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.QueuedOperation;
import build.buildfarm.v1test.WorkerConfig;
import build.buildfarm.worker.ExecutionPolicies;
import build.buildfarm.worker.OutputDirectory;
import build.buildfarm.worker.ResourceLimits;
import build.buildfarm.worker.UploadManifest;
import build.buildfarm.worker.WorkerContext;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.google.common.hash.HashCode;
import com.google.longrunning.Operation;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Deadline;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

class OperationQueueWorkerContext implements WorkerContext {
  private static final Logger logger = Logger.getLogger(WorkerContext.class.getName());

  private final WorkerConfig config;
  private final OperationQueueClient oq;
  private final Instance casInstance;
  private final Instance acInstance;
  private final ByteStreamUploader uploader;
  private final CASFileCache fileCache;
  private final Path root;
  private final Retrier retrier;
  private final ListMultimap<String, ExecutionPolicy> policies;
  private final Map<Path, Iterable<String>> rootInputFiles = new ConcurrentHashMap<>();
  private final Map<Path, Iterable<Digest>> rootInputDirectories = new ConcurrentHashMap<>();

  OperationQueueWorkerContext(
      WorkerConfig config,
      Instance casInstance,
      Instance acInstance,
      OperationQueueClient oq,
      ByteStreamUploader uploader,
      CASFileCache fileCache,
      Path root,
      Retrier retrier) {
    this.config = config;
    this.casInstance = casInstance;
    this.acInstance = acInstance;
    this.oq = oq;
    this.uploader = uploader;
    this.fileCache = fileCache;
    this.root = root;
    this.retrier = retrier;
    policies = ExecutionPolicies.toMultimap(config.getExecutionPoliciesList());
  }

  @Override
  public String getName() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean shouldErrorOperationOnRemainingResources() {
    return config.getErrorOperationRemainingResources();
  }

  @Override
  public Poller createPoller(String name, QueueEntry queueEntry, ExecutionStage.Value stage) {
    Poller poller = new Poller(config.getOperationPollPeriod());
    resumePoller(poller, name, queueEntry, stage, () -> {}, Deadline.after(10, DAYS));
    return poller;
  }

  @Override
  public void resumePoller(
      Poller poller,
      String name,
      QueueEntry queueEntry,
      ExecutionStage.Value stage,
      Runnable onFailure,
      Deadline deadline) {
    String operationName = queueEntry.getExecuteEntry().getOperationName();
    poller.resume(
        () -> {
          boolean success = oq.poll(operationName, stage);
          logger.log(
              INFO,
              format(
                  "%s: poller: Completed Poll for %s: %s",
                  name, operationName, success ? "OK" : "Failed"));
          if (!success) {
            onFailure.run();
          }
          return success;
        },
        () -> {
          logger.log(INFO, format("%s: poller: Deadline expired for %s", name, operationName));
          onFailure.run();
        },
        deadline);
  }

  @Override
  public DigestUtil getDigestUtil() {
    return casInstance.getDigestUtil();
  }

  @Override
  public void match(MatchListener listener) throws InterruptedException {
    oq.match(listener);
  }

  @Override
  public CASInsertionPolicy getFileCasPolicy() {
    return config.getFileCasPolicy();
  }

  @Override
  public CASInsertionPolicy getStdoutCasPolicy() {
    return config.getStdoutCasPolicy();
  }

  @Override
  public CASInsertionPolicy getStderrCasPolicy() {
    return config.getStderrCasPolicy();
  }

  @Override
  public int getInputFetchStageWidth() {
    return config.getInputFetchStageWidth();
  }

  @Override
  public int getExecuteStageWidth() {
    return config.getExecuteStageWidth();
  }

  @Override
  public boolean hasDefaultActionTimeout() {
    return config.hasDefaultActionTimeout();
  }

  @Override
  public boolean hasMaximumActionTimeout() {
    return config.hasMaximumActionTimeout();
  }

  @Override
  public boolean getStreamStdout() {
    return config.getStreamStdout();
  }

  @Override
  public boolean getStreamStderr() {
    return config.getStreamStderr();
  }

  @Override
  public Duration getDefaultActionTimeout() {
    return config.getDefaultActionTimeout();
  }

  @Override
  public Duration getMaximumActionTimeout() {
    return config.getMaximumActionTimeout();
  }

  @Override
  public void uploadOutputs(
      Digest actionDigest,
      ActionResult.Builder resultBuilder,
      Path actionRoot,
      Iterable<String> outputFiles,
      Iterable<String> outputDirs)
      throws IOException, InterruptedException {
    uploadOutputs(
        resultBuilder,
        casInstance.getDigestUtil(),
        actionRoot,
        outputFiles,
        outputDirs,
        uploader,
        config.getInlineContentLimit(),
        config.getFileCasPolicy(),
        config.getStdoutCasPolicy(),
        config.getStderrCasPolicy());
  }

  @Override
  public QueuedOperation getQueuedOperation(QueueEntry queueEntry)
      throws IOException, InterruptedException {
    Digest queuedOperationDigest = queueEntry.getQueuedOperationDigest();
    ByteString queuedOperationBlob =
        getBlob(
            casInstance, queuedOperationDigest, queueEntry.getExecuteEntry().getRequestMetadata());
    if (queuedOperationBlob == null) {
      return null;
    }
    try {
      return QueuedOperation.parseFrom(queuedOperationBlob);
    } catch (InvalidProtocolBufferException e) {
      logger.log(
          WARNING,
          format(
              "invalid queued operation: %s(%s)",
              queueEntry.getExecuteEntry().getOperationName(),
              DigestUtil.toString(queuedOperationDigest)));
      return null;
    }
  }

  @Override
  public Path createExecDir(
      String operationName, Map<Digest, Directory> directoriesIndex, Action action, Command command)
      throws IOException, InterruptedException {
    OutputDirectory outputDirectory =
        OutputDirectory.parse(
            command.getOutputFilesList(),
            command.getOutputDirectoriesList(),
            command.getEnvironmentVariablesList());

    Path execDir = root.resolve(operationName);
    if (Files.exists(execDir)) {
      Directories.remove(execDir);
    }
    Files.createDirectories(execDir);

    ImmutableList.Builder<String> inputFiles = new ImmutableList.Builder<>();
    ImmutableList.Builder<Digest> inputDirectories = new ImmutableList.Builder<>();

    boolean fetched = false;
    try {
      fetchInputs(
          execDir,
          action.getInputRootDigest(),
          directoriesIndex,
          outputDirectory,
          inputFiles,
          inputDirectories);
      fetched = true;
    } finally {
      if (!fetched) {
        fileCache.decrementReferences(inputFiles.build(), inputDirectories.build());
      }
    }

    rootInputFiles.put(execDir, inputFiles.build());
    rootInputDirectories.put(execDir, inputDirectories.build());

    boolean stamped = false;
    try {
      outputDirectory.stamp(execDir);
      stamped = true;
    } finally {
      if (!stamped) {
        destroyExecDir(execDir);
      }
    }

    return execDir;
  }

  @Override
  public void destroyExecDir(Path execDir) throws IOException, InterruptedException {
    Iterable<String> inputFiles = rootInputFiles.remove(execDir);
    Iterable<Digest> inputDirectories = rootInputDirectories.remove(execDir);

    fileCache.decrementReferences(inputFiles, inputDirectories);
    Directories.remove(execDir);
  }

  @Override
  public Iterable<ExecutionPolicy> getExecutionPolicies(String name) {
    return policies.get(name);
  }

  @Override
  public boolean putOperation(Operation operation, Action action) throws InterruptedException {
    return oq.put(operation);
  }

  // doesn't belong in CAS or AC, must be in OQ
  @Override
  public Write getOperationStreamWrite(String name) {
    return oq.getStreamWrite(name);
  }

  @Override
  public void blacklistAction(String actionId) {
    // ignore
  }

  @Override
  public void putActionResult(ActionKey actionKey, ActionResult actionResult)
      throws InterruptedException {
    try {
      retrier.execute(
          () -> {
            acInstance.putActionResult(actionKey, actionResult);
            return null;
          });
    } catch (IOException e) {
      Throwable cause = e.getCause();
      if (cause == null) {
        throw new RuntimeException(e);
      }
      Throwables.throwIfUnchecked(cause);
      throw new RuntimeException(cause);
    }
  }

  @Override
  public long getStandardOutputLimit() {
    return Size.mbToBytes(100);
  }

  @Override
  public long getStandardErrorLimit() {
    return Size.mbToBytes(100);
  }

  @Override
  public void createExecutionLimits() {}

  @Override
  public void destroyExecutionLimits() {}

  @Override
  public IOResource limitExecution(
      String operationName, ImmutableList.Builder<String> arguments, Command command) {
    return new IOResource() {
      @Override
      public void close() {}

      @Override
      public boolean isReferenced() {
        return false;
      }
    };
  }

  @Override
  public int commandExecutionClaims(Command command) {
    return 1;
  }

  @Override
  public ResourceLimits commandExecutionSettings(Command command) {
    return new ResourceLimits();
  }

  private static void uploadManifest(UploadManifest manifest, ByteStreamUploader uploader)
      throws IOException, InterruptedException {
    Map<HashCode, Chunker> filesToUpload = Maps.newHashMap();

    Map<Digest, Path> digestToFile = manifest.getDigestToFile();
    Map<Digest, Chunker> digestToChunkers = manifest.getDigestToChunkers();
    Collection<Digest> digests = new ArrayList<>();
    digests.addAll(digestToFile.keySet());
    digests.addAll(digestToChunkers.keySet());

    for (Digest digest : digests) {
      Chunker chunker;
      Path file = digestToFile.get(digest);
      if (file != null) {
        chunker = Chunker.builder().setInput(digest.getSizeBytes(), file).build();
      } else {
        chunker = digestToChunkers.get(digest);
        if (chunker == null) {
          String message = "FindMissingBlobs call returned an unknown digest: " + digest;
          throw new IOException(message);
        }
      }
      filesToUpload.put(HashCode.fromString(digest.getHash()), chunker);
    }

    if (!filesToUpload.isEmpty()) {
      uploader.uploadBlobs(filesToUpload);
    }
  }

  private static UploadManifest createManifest(
      ActionResult.Builder result,
      DigestUtil digestUtil,
      Path execRoot,
      Iterable<String> outputFiles,
      Iterable<String> outputDirs,
      int inlineContentLimit,
      CASInsertionPolicy fileCasPolicy,
      CASInsertionPolicy stdoutCasPolicy,
      CASInsertionPolicy stderrCasPolicy)
      throws IOException, InterruptedException {
    UploadManifest manifest =
        new UploadManifest(
            digestUtil, result, execRoot, /* allowSymlinks= */ true, inlineContentLimit);

    manifest.addFiles(
        Iterables.transform(outputFiles, (file) -> execRoot.resolve(file)), fileCasPolicy);
    manifest.addDirectories(Iterables.transform(outputDirs, (dir) -> execRoot.resolve(dir)));

    /* put together our outputs and update the result */
    if (result.getStdoutRaw().size() > 0) {
      manifest.addContent(
          result.getStdoutRaw(), stdoutCasPolicy, result::setStdoutRaw, result::setStdoutDigest);
    }
    if (result.getStderrRaw().size() > 0) {
      manifest.addContent(
          result.getStderrRaw(), stderrCasPolicy, result::setStderrRaw, result::setStderrDigest);
    }

    return manifest;
  }

  @VisibleForTesting
  static void uploadOutputs(
      ActionResult.Builder resultBuilder,
      DigestUtil digestUtil,
      Path actionRoot,
      Iterable<String> outputFiles,
      Iterable<String> outputDirs,
      ByteStreamUploader uploader,
      int inlineContentLimit,
      CASInsertionPolicy fileCasPolicy,
      CASInsertionPolicy stdoutCasPolicy,
      CASInsertionPolicy stderrCasPolicy)
      throws IOException, InterruptedException {
    uploadManifest(
        createManifest(
            resultBuilder,
            digestUtil,
            actionRoot,
            outputFiles,
            outputDirs,
            inlineContentLimit,
            fileCasPolicy,
            stdoutCasPolicy,
            stderrCasPolicy),
        uploader);
  }

  private void fetchInputs(
      Path execDir,
      Digest inputRoot,
      Map<Digest, Directory> directoriesIndex,
      OutputDirectory outputDirectory,
      ImmutableList.Builder<String> inputFiles,
      ImmutableList.Builder<Digest> inputDirectories)
      throws IOException, InterruptedException {
    Directory directory;
    if (inputRoot.getSizeBytes() == 0) {
      directory = Directory.getDefaultInstance();
    } else {
      directory = directoriesIndex.get(inputRoot);
      if (directory == null) {
        throw new IOException(
            "Directory " + DigestUtil.toString(inputRoot) + " is not in input index");
      }
    }

    getInterruptiblyOrIOException(
        allAsList(
            fileCache.putFiles(
                directory.getFilesList(),
                directory.getSymlinksList(),
                execDir,
                inputFiles,
                newDirectExecutorService())));
    for (DirectoryNode directoryNode : directory.getDirectoriesList()) {
      Digest digest = directoryNode.getDigest();
      String name = directoryNode.getName();
      OutputDirectory childOutputDirectory =
          outputDirectory != null ? outputDirectory.getChild(name) : null;
      Path dirPath = execDir.resolve(name);
      if (childOutputDirectory != null || !config.getLinkInputDirectories()) {
        Files.createDirectories(dirPath);
        fetchInputs(
            dirPath, digest, directoriesIndex, childOutputDirectory, inputFiles, inputDirectories);
      } else {
        inputDirectories.add(digest);
        linkDirectory(dirPath, digest, directoriesIndex);
      }
    }
  }

  private void linkDirectory(Path execPath, Digest digest, Map<Digest, Directory> directoriesIndex)
      throws IOException, InterruptedException {
    Path cachePath =
        getInterruptiblyOrIOException(
            fileCache.putDirectory(digest, directoriesIndex, newDirectExecutorService()));
    Files.createSymbolicLink(execPath, cachePath);
  }
}
