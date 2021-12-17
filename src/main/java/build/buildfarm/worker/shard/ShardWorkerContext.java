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

import static build.buildfarm.cas.ContentAddressableStorage.UNLIMITED_ENTRY_SIZE_MAX;
import static build.buildfarm.common.Actions.checkPreconditionFailure;
import static build.buildfarm.common.Errors.VIOLATION_TYPE_INVALID;
import static build.buildfarm.common.Errors.VIOLATION_TYPE_MISSING;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.DAYS;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.ExecutionStage;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.Platform;
import build.bazel.remote.execution.v2.Tree;
import build.buildfarm.backplane.Backplane;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.EntryLimitException;
import build.buildfarm.common.ExecutionWrappers;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.common.LinuxSandboxOptions;
import build.buildfarm.common.Poller;
import build.buildfarm.common.Size;
import build.buildfarm.common.Write;
import build.buildfarm.common.grpc.Retrier;
import build.buildfarm.common.grpc.Retrier.Backoff;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.MatchListener;
import build.buildfarm.v1test.CASInsertionPolicy;
import build.buildfarm.v1test.ExecutionPolicy;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.QueuedOperation;
import build.buildfarm.worker.DequeueMatchEvaluator;
import build.buildfarm.worker.DequeueMatchSettings;
import build.buildfarm.worker.ExecutionPolicies;
import build.buildfarm.worker.RetryingMatchListener;
import build.buildfarm.worker.WorkerContext;
import build.buildfarm.worker.cgroup.Cpu;
import build.buildfarm.worker.cgroup.Group;
import build.buildfarm.worker.cgroup.Mem;
import build.buildfarm.worker.resources.ResourceDecider;
import build.buildfarm.worker.resources.ResourceLimits;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.longrunning.Operation;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.PreconditionFailure;
import io.grpc.Deadline;
import io.grpc.Status;
import io.grpc.StatusException;
import io.prometheus.client.Counter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.logging.Level;
import java.util.logging.Logger;

class ShardWorkerContext implements WorkerContext {
  private static final Logger logger = Logger.getLogger(ShardWorkerContext.class.getName());

  private static final String PROVISION_CORES_NAME = "cores";

  private static final Counter completedOperations =
      Counter.build().name("completed_operations").help("Completed operations.").register();
  private static final Counter operationPollerCounter =
      Counter.build().name("operation_poller").help("Number of operations polled.").register();

  private final String name;
  private final Platform platform;
  private final DequeueMatchSettings matchSettings;
  private final SetMultimap<String, String> matchProvisions;
  private final Duration operationPollPeriod;
  private final OperationPoller operationPoller;
  private final int inputFetchDeadline;
  private final int inputFetchStageWidth;
  private final int executeStageWidth;
  private final Backplane backplane;
  private final ExecFileSystem execFileSystem;
  private final InputStreamFactory inputStreamFactory;
  private final ListMultimap<String, ExecutionPolicy> policies;
  private final Instance instance;
  private final Duration defaultActionTimeout;
  private final Duration maximumActionTimeout;
  private final boolean limitExecution;
  private final boolean limitGlobalExecution;
  private final boolean onlyMulticoreTests;
  private final Map<String, QueueEntry> activeOperations = Maps.newConcurrentMap();
  private final Group executionsGroup = Group.getRoot().getChild("executions");
  private final Group operationsGroup = executionsGroup.getChild("operations");
  private final CasWriter writer;
  private final boolean errorOperationRemainingResources;

  static SetMultimap<String, String> getMatchProvisions(
      Platform platform, Iterable<ExecutionPolicy> policies, int executeStageWidth) {
    ImmutableSetMultimap.Builder<String, String> provisions = ImmutableSetMultimap.builder();
    Platform matchPlatform = ExecutionPolicies.getMatchPlatform(platform, policies);
    for (Platform.Property property : matchPlatform.getPropertiesList()) {
      provisions.put(property.getName(), property.getValue());
    }
    provisions.put(PROVISION_CORES_NAME, String.format("%d", executeStageWidth));
    return provisions.build();
  }

  ShardWorkerContext(
      String name,
      DequeueMatchSettings matchSettings,
      Platform platform,
      Duration operationPollPeriod,
      OperationPoller operationPoller,
      int inputFetchStageWidth,
      int executeStageWidth,
      int inputFetchDeadline,
      Backplane backplane,
      ExecFileSystem execFileSystem,
      InputStreamFactory inputStreamFactory,
      Iterable<ExecutionPolicy> policies,
      Instance instance,
      Duration defaultActionTimeout,
      Duration maximumActionTimeout,
      boolean limitExecution,
      boolean limitGlobalExecution,
      boolean onlyMulticoreTests,
      boolean errorOperationRemainingResources,
      CasWriter writer) {
    this.name = name;
    this.matchSettings = matchSettings;
    this.platform = platform;
    this.matchProvisions = getMatchProvisions(platform, policies, executeStageWidth);
    this.operationPollPeriod = operationPollPeriod;
    this.operationPoller = operationPoller;
    this.inputFetchStageWidth = inputFetchStageWidth;
    this.executeStageWidth = executeStageWidth;
    this.inputFetchDeadline = inputFetchDeadline;
    this.backplane = backplane;
    this.execFileSystem = execFileSystem;
    this.inputStreamFactory = inputStreamFactory;
    this.policies = ExecutionPolicies.toMultimap(policies);
    this.instance = instance;
    this.defaultActionTimeout = defaultActionTimeout;
    this.maximumActionTimeout = maximumActionTimeout;
    this.limitExecution = limitExecution;
    this.limitGlobalExecution = limitGlobalExecution;
    this.onlyMulticoreTests = onlyMulticoreTests;
    this.errorOperationRemainingResources = errorOperationRemainingResources;
    this.writer = writer;
    Preconditions.checkState(
        !limitGlobalExecution || limitExecution,
        "limit_global_execution is meaningless without limit_execution");
    Preconditions.checkState(
        !onlyMulticoreTests || limitExecution,
        "only_multicore_tests is meaningless without limit_execution");
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
  public boolean shouldErrorOperationOnRemainingResources() {
    return errorOperationRemainingResources;
  }

  @Override
  public Poller createPoller(String name, QueueEntry queueEntry, ExecutionStage.Value stage) {
    Poller poller = new Poller(operationPollPeriod);
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
          boolean success = false;
          try {
            success =
                operationPoller.poll(queueEntry, stage, System.currentTimeMillis() + 30 * 1000);
          } catch (IOException e) {
            logger.log(
                Level.SEVERE, format("%s: poller: error while polling %s", name, operationName), e);
          }
          if (!success) {
            logger.log(
                Level.INFO,
                format("%s: poller: Completed Poll for %s: Failed", name, operationName));
            onFailure.run();
          } else {
            operationPollerCounter.inc();
            logger.log(
                Level.INFO, format("%s: poller: Completed Poll for %s: OK", name, operationName));
          }
          return success;
        },
        () -> {
          logger.log(
              Level.INFO, format("%s: poller: Deadline expired for %s", name, operationName));
          onFailure.run();
        },
        deadline);
  }

  @Override
  public DigestUtil getDigestUtil() {
    return instance.getDigestUtil();
  }

  private ByteString getBlob(Digest digest) throws IOException, InterruptedException {
    try (InputStream in = inputStreamFactory.newInput(digest, 0)) {
      return ByteString.readFrom(in);
    } catch (NoSuchFileException e) {
      return null;
    }
  }

  @Override
  public QueuedOperation getQueuedOperation(QueueEntry queueEntry)
      throws IOException, InterruptedException {
    Digest queuedOperationDigest = queueEntry.getQueuedOperationDigest();
    ByteString queuedOperationBlob = getBlob(queuedOperationDigest);
    if (queuedOperationBlob == null) {
      logger.log(
          Level.WARNING,
          format(
              "missing queued operation: %s(%s)",
              queueEntry.getExecuteEntry().getOperationName(),
              DigestUtil.toString(queuedOperationDigest)));
      return null;
    }
    try {
      return QueuedOperation.parseFrom(queuedOperationBlob);
    } catch (InvalidProtocolBufferException e) {
      logger.log(
          Level.WARNING,
          format(
              "invalid queued operation: %s(%s)",
              queueEntry.getExecuteEntry().getOperationName(),
              DigestUtil.toString(queuedOperationDigest)));
      return null;
    }
  }

  @SuppressWarnings("ConstantConditions")
  private void matchInterruptible(MatchListener listener) throws IOException, InterruptedException {
    listener.onWaitStart();
    QueueEntry queueEntry = null;
    try {
      queueEntry = backplane.dispatchOperation(platform.getPropertiesList());
    } catch (IOException e) {
      Status status = Status.fromThrowable(e);
      switch (status.getCode()) {
        case DEADLINE_EXCEEDED:
          logger.log(Level.WARNING, "backplane timed out for match during bookkeeping");
          break;
        case UNAVAILABLE:
          logger.log(Level.WARNING, "backplane was unavailable for match");
          break;
        default:
          throw e;
      }
      // transient backplane errors will propagate a null queueEntry
    }
    listener.onWaitEnd();

    if (queueEntry == null
        || DequeueMatchEvaluator.shouldKeepOperation(matchSettings, matchProvisions, queueEntry)) {
      listener.onEntry(queueEntry);
    } else {
      backplane.rejectOperation(queueEntry);
    }
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
  }

  @Override
  public void match(MatchListener listener) throws InterruptedException {
    RetryingMatchListener dedupMatchListener =
        new RetryingMatchListener() {
          boolean matched = false;

          @Override
          public boolean getMatched() {
            return !matched;
          }

          @Override
          public void onWaitStart() {
            listener.onWaitStart();
          }

          @Override
          public void onWaitEnd() {
            listener.onWaitEnd();
          }

          @Override
          public boolean onEntry(QueueEntry queueEntry) throws InterruptedException {
            if (queueEntry == null) {
              matched = true;
              return listener.onEntry(null);
            }
            String operationName = queueEntry.getExecuteEntry().getOperationName();
            if (activeOperations.putIfAbsent(operationName, queueEntry) != null) {
              logger.log(Level.WARNING, "matched duplicate operation " + operationName);
              return false;
            }
            matched = true;
            boolean success = listener.onEntry(queueEntry);
            if (!success) {
              requeue(operationName);
            }
            return success;
          }

          @Override
          public void onError(Throwable t) {
            Throwables.throwIfUnchecked(t);
            throw new RuntimeException(t);
          }

          @Override
          public void setOnCancelHandler(Runnable onCancelHandler) {
            listener.setOnCancelHandler(onCancelHandler);
          }
        };
    while (dedupMatchListener.getMatched()) {
      try {
        matchInterruptible(dedupMatchListener);
      } catch (IOException e) {
        throw Status.fromThrowable(e).asRuntimeException();
      }
    }
  }

  private void requeue(String operationName) {
    QueueEntry queueEntry = activeOperations.remove(operationName);
    try {
      operationPoller.poll(queueEntry, ExecutionStage.Value.QUEUED, 0);
    } catch (IOException e) {
      // ignore, at least dispatcher will pick us up in 30s
      logger.log(Level.SEVERE, "Failure while trying to fast requeue " + operationName, e);
    }
  }

  void requeue(Operation operation) {
    requeue(operation.getName());
  }

  void deactivate(String operationName) {
    activeOperations.remove(operationName);
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
  public int getInputFetchStageWidth() {
    return inputFetchStageWidth;
  }

  @Override
  public int getExecuteStageWidth() {
    return executeStageWidth;
  }

  @Override
  public int getInputFetchDeadline() {
    return inputFetchDeadline;
  }

  @Override
  public boolean hasDefaultActionTimeout() {
    return defaultActionTimeout.getSeconds() > 0 || defaultActionTimeout.getNanos() > 0;
  }

  @Override
  public boolean hasMaximumActionTimeout() {
    return maximumActionTimeout.getSeconds() > 0 || maximumActionTimeout.getNanos() > 0;
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
    return defaultActionTimeout;
  }

  @Override
  public Duration getMaximumActionTimeout() {
    return maximumActionTimeout;
  }

  private void insertBlob(Digest digest, ByteString content)
      throws IOException, InterruptedException {
    if (digest.getSizeBytes() > 0) {
      writer.insertBlob(digest, content);
    }
  }

  private void insertFile(Digest digest, Path file) throws IOException, InterruptedException {
    writer.write(digest, file);
  }

  private void updateActionResultStdOutputs(ActionResult.Builder resultBuilder)
      throws IOException, InterruptedException {
    ByteString stdoutRaw = resultBuilder.getStdoutRaw();
    if (stdoutRaw.size() > 0) {
      // reset to allow policy to determine inlining
      resultBuilder.setStdoutRaw(ByteString.EMPTY);
      Digest stdoutDigest = getDigestUtil().compute(stdoutRaw);
      insertBlob(stdoutDigest, stdoutRaw);
      resultBuilder.setStdoutDigest(stdoutDigest);
    }

    ByteString stderrRaw = resultBuilder.getStderrRaw();
    if (stderrRaw.size() > 0) {
      // reset to allow policy to determine inlining
      resultBuilder.setStderrRaw(ByteString.EMPTY);
      Digest stderrDigest = getDigestUtil().compute(stderrRaw);
      insertBlob(stderrDigest, stderrRaw);
      resultBuilder.setStderrDigest(stderrDigest);
    }
  }

  private void uploadOutputFile(
      ActionResult.Builder resultBuilder,
      String outputFile,
      Path actionRoot,
      PreconditionFailure.Builder preconditionFailure)
      throws IOException, InterruptedException {
    Path outputPath = actionRoot.resolve(outputFile);
    if (!Files.exists(outputPath)) {
      logger.log(Level.FINE, "ReportResultStage: " + outputFile + " does not exist...");
      return;
    }

    if (Files.isDirectory(outputPath)) {
      logger.log(Level.FINE, "ReportResultStage: " + outputFile + " is a directory");
      preconditionFailure
          .addViolationsBuilder()
          .setType(VIOLATION_TYPE_INVALID)
          .setSubject(outputFile)
          .setDescription("An output file was a directory");
      return;
    }

    long size = Files.size(outputPath);
    long maxEntrySize = execFileSystem.getStorage().maxEntrySize();
    if (maxEntrySize != UNLIMITED_ENTRY_SIZE_MAX && size > maxEntrySize) {
      preconditionFailure
          .addViolationsBuilder()
          .setType(VIOLATION_TYPE_MISSING)
          .setSubject(outputFile + ": " + size)
          .setDescription(
              "An output could not be uploaded because it exceeded the maximum size of an entry");
      return;
    }

    // will run into issues if we end up blocking on the cache insertion, might
    // want to decrement input references *before* this to ensure that we cannot
    // cause an internal deadlock

    Digest digest;
    try {
      digest = getDigestUtil().compute(outputPath);
    } catch (NoSuchFileException e) {
      return;
    }

    resultBuilder
        .addOutputFilesBuilder()
        .setPath(outputFile)
        .setDigest(digest)
        .setIsExecutable(Files.isExecutable(outputPath));

    try {
      insertFile(digest, outputPath);
    } catch (EntryLimitException e) {
      preconditionFailure
          .addViolationsBuilder()
          .setType(VIOLATION_TYPE_MISSING)
          .setSubject("blobs/" + DigestUtil.toString(digest))
          .setDescription(
              "An output could not be uploaded because it exceeded the maximum size of an entry");
    }
  }

  @VisibleForTesting
  static class OutputDirectoryContext {
    private final List<FileNode> files = new ArrayList<>();
    private final List<DirectoryNode> directories = new ArrayList<>();

    void addFile(FileNode fileNode) {
      files.add(fileNode);
    }

    void addDirectory(DirectoryNode directoryNode) {
      directories.add(directoryNode);
    }

    Directory toDirectory() {
      files.sort(Comparator.comparing(FileNode::getName));
      directories.sort(Comparator.comparing(DirectoryNode::getName));
      return Directory.newBuilder().addAllFiles(files).addAllDirectories(directories).build();
    }
  }

  private void uploadOutputDirectory(
      ActionResult.Builder resultBuilder,
      String outputDir,
      Path actionRoot,
      PreconditionFailure.Builder preconditionFailure)
      throws IOException, InterruptedException {
    Path outputDirPath = actionRoot.resolve(outputDir);
    if (!Files.exists(outputDirPath)) {
      logger.log(Level.FINE, "ReportResultStage: " + outputDir + " does not exist...");
      return;
    }

    if (!Files.isDirectory(outputDirPath)) {
      logger.log(Level.FINE, "ReportResultStage: " + outputDir + " is not a directory...");
      preconditionFailure
          .addViolationsBuilder()
          .setType(VIOLATION_TYPE_INVALID)
          .setSubject(outputDir)
          .setDescription("An output directory was not a directory");
      return;
    }

    Tree.Builder treeBuilder = Tree.newBuilder();
    OutputDirectoryContext outputRoot = new OutputDirectoryContext();
    Files.walkFileTree(
        outputDirPath,
        new SimpleFileVisitor<Path>() {
          OutputDirectoryContext currentDirectory = null;
          final Stack<OutputDirectoryContext> path = new Stack<>();

          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            Digest digest;
            try {
              digest = getDigestUtil().compute(file);
            } catch (NoSuchFileException e) {
              logger.log(
                  Level.SEVERE,
                  format(
                      "error visiting file %s under output dir %s",
                      outputDirPath.relativize(file), outputDirPath.toAbsolutePath()),
                  e);
              return FileVisitResult.CONTINUE;
            }

            // should we cast to PosixFilePermissions and do gymnastics there for executable?

            // TODO symlink per revision proposal
            currentDirectory.addFile(
                FileNode.newBuilder()
                    .setName(file.getFileName().toString())
                    .setDigest(digest)
                    .setIsExecutable(Files.isExecutable(file))
                    .build());
            try {
              insertFile(digest, file);
            } catch (InterruptedException e) {
              throw new IOException(e);
            } catch (EntryLimitException e) {
              preconditionFailure
                  .addViolationsBuilder()
                  .setType(VIOLATION_TYPE_MISSING)
                  .setSubject("blobs/" + DigestUtil.toString(digest))
                  .setDescription(
                      "An output could not be uploaded because it exceeded the maximum size of an entry");
            }
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
            path.push(currentDirectory);
            if (dir.equals(outputDirPath)) {
              currentDirectory = outputRoot;
            } else {
              currentDirectory = new OutputDirectoryContext();
            }
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
            OutputDirectoryContext parentDirectory = path.pop();
            Directory directory = currentDirectory.toDirectory();
            if (parentDirectory == null) {
              treeBuilder.setRoot(directory);
            } else {
              parentDirectory.addDirectory(
                  DirectoryNode.newBuilder()
                      .setName(dir.getFileName().toString())
                      .setDigest(getDigestUtil().compute(directory))
                      .build());
              treeBuilder.addChildren(directory);
            }
            currentDirectory = parentDirectory;
            return FileVisitResult.CONTINUE;
          }
        });
    Tree tree = treeBuilder.build();
    ByteString treeBlob = tree.toByteString();
    Digest treeDigest = getDigestUtil().compute(treeBlob);
    insertBlob(treeDigest, treeBlob);
    resultBuilder.addOutputDirectoriesBuilder().setPath(outputDir).setTreeDigest(treeDigest);
  }

  @Override
  public void uploadOutputs(
      Digest actionDigest,
      ActionResult.Builder resultBuilder,
      Path actionRoot,
      Iterable<String> outputFiles,
      Iterable<String> outputDirs)
      throws IOException, InterruptedException, StatusException {
    PreconditionFailure.Builder preconditionFailure = PreconditionFailure.newBuilder();
    for (String outputFile : outputFiles) {
      uploadOutputFile(resultBuilder, outputFile, actionRoot, preconditionFailure);
    }
    for (String outputDir : outputDirs) {
      uploadOutputDirectory(resultBuilder, outputDir, actionRoot, preconditionFailure);
    }
    checkPreconditionFailure(actionDigest, preconditionFailure.build());

    /* put together our outputs and update the result */
    updateActionResultStdOutputs(resultBuilder);
  }

  @Override
  public Iterable<ExecutionPolicy> getExecutionPolicies(String name) {
    return policies.get(name);
  }

  @Override
  public boolean putOperation(Operation operation) throws IOException, InterruptedException {
    boolean success = createBackplaneRetrier().execute(() -> instance.putOperation(operation));
    if (success && operation.getDone()) {
      completedOperations.inc();
      logger.log(Level.FINE, "CompletedOperation: " + operation.getName());
    }
    return success;
  }

  @Override
  public Path createExecDir(
      String operationName, Map<Digest, Directory> directoriesIndex, Action action, Command command)
      throws IOException, InterruptedException {
    return execFileSystem.createExecDir(operationName, directoriesIndex, action, command);
  }

  // might want to split for removeDirectory and decrement references to avoid removing for streamed
  // output
  @Override
  public void destroyExecDir(Path execDir) throws IOException, InterruptedException {
    execFileSystem.destroyExecDir(execDir);
  }

  @Override
  public void blacklistAction(String actionId) throws IOException, InterruptedException {
    createBackplaneRetrier()
        .execute(
            () -> {
              backplane.blacklistAction(actionId);
              return null;
            });
  }

  @Override
  public void putActionResult(ActionKey actionKey, ActionResult actionResult)
      throws IOException, InterruptedException {
    createBackplaneRetrier()
        .execute(
            () -> {
              instance.putActionResult(actionKey, actionResult);
              return null;
            });
  }

  @Override
  public Write getOperationStreamWrite(String name) {
    throw new UnsupportedOperationException();
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
  public void createExecutionLimits() {
    if (limitExecution) {
      createOperationExecutionLimits();
    }
  }

  void createOperationExecutionLimits() {
    try {
      int availableProcessors = Runtime.getRuntime().availableProcessors();
      Preconditions.checkState(availableProcessors >= executeStageWidth);
      int executionsShares =
          Group.getRoot().getCpu().getShares() * executeStageWidth / availableProcessors;
      executionsGroup.getCpu().setShares(executionsShares);
      if (executeStageWidth < availableProcessors) {
        /* only divide up our cfs quota if we need to limit below the available processors for executions */
        executionsGroup
            .getCpu()
            .setCFSQuota(executeStageWidth * Group.getRoot().getCpu().getCFSPeriod());
      }
      // create 1024 * execution width shares to choose from
      operationsGroup.getCpu().setShares(executeStageWidth * 1024);
    } catch (IOException e) {
      try {
        operationsGroup.getCpu().close();
      } catch (IOException closeEx) {
        e.addSuppressed(closeEx);
      }
      throw new RuntimeException(e);
    }
  }

  @Override
  public void destroyExecutionLimits() {
    try {
      operationsGroup.getCpu().close();
      executionsGroup.getCpu().close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  String getOperationId(String operationName) {
    String[] components = operationName.split("/");
    Preconditions.checkState(components.length >= 3);
    Preconditions.checkState(components[components.length - 2].equals("operations"));
    return components[components.length - 1];
  }

  @Override
  public int commandExecutionClaims(Command command) {
    return commandExecutionSettings(command).cpu.claimed;
  }

  public ResourceLimits commandExecutionSettings(Command command) {
    return ResourceDecider.decideResourceLimitations(
        command, name, onlyMulticoreTests, limitGlobalExecution, getExecuteStageWidth());
  }

  @Override
  public IOResource limitExecution(
      String operationName,
      ImmutableList.Builder<String> arguments,
      Command command,
      Path workingDirectory) {
    if (limitExecution) {
      ResourceLimits limits = commandExecutionSettings(command);
      return limitSpecifiedExecution(limits, operationName, arguments, workingDirectory);
    }
    return new IOResource() {
      @Override
      public void close() {}

      @Override
      public boolean isReferenced() {
        return false;
      }
    };
  }

  IOResource limitSpecifiedExecution(
      ResourceLimits limits,
      String operationName,
      ImmutableList.Builder<String> arguments,
      Path workingDirectory) {
    // The decision to apply resource restrictions has already been decided within the
    // ResourceLimits object. We apply the cgroup settings to file resources
    // and collect group names to use on the CLI.
    String operationId = getOperationId(operationName);
    final Group group = operationsGroup.getChild(operationId);
    ArrayList<IOResource> resources = new ArrayList<>();
    ArrayList<String> usedGroups = new ArrayList<>();

    // Possibly set core restrictions.
    if (limits.cpu.limit) {
      applyCpuLimits(group, limits, resources);
      usedGroups.add(group.getCpu().getName());
    }

    // Possibly set memory restrictions.
    if (limits.mem.limit) {
      applyMemLimits(group, limits, resources);
      usedGroups.add(group.getMem().getName());
    }

    // Decide the CLI for running under cgroups
    if (!usedGroups.isEmpty()) {
      arguments.add(
          ExecutionWrappers.CGROUPS,
          "-g",
          String.join(",", usedGroups) + ":" + group.getHierarchy());
    }

    // Possibly set network restrictions.
    // This is not the ideal implementation of block-network.
    // For now, without the linux-sandbox, we will unshare the network namespace.
    if (limits.network.blockNetwork && !limits.useLinuxSandbox) {
      arguments.add(ExecutionWrappers.UNSHARE, "-n", "-r");
    }

    // Decide the CLI for running the sandbox
    // For reference on how bazel spawns the sandbox:
    // https://github.com/bazelbuild/bazel/blob/ddf302e2798be28bb67e32d5c2fc9c73a6a1fbf4/src/main/java/com/google/devtools/build/lib/sandbox/LinuxSandboxUtil.java#L183
    if (limits.useLinuxSandbox) {
      LinuxSandboxOptions options = decideLinuxSandboxOptions(limits, workingDirectory);
      addLinuxSandboxCli(arguments, options);
    }

    if (limits.time.skipSleep) {
      arguments.add(ExecutionWrappers.SKIP_SLEEP);

      // we set these values very high because we want sleep calls to return immediately.
      arguments.add("90000000"); // delay factor
      arguments.add("90000000"); // time factor
      arguments.add(ExecutionWrappers.SKIP_SLEEP_PRELOAD);

      if (limits.time.timeShift != 0) {
        arguments.add(ExecutionWrappers.DELAY);
        arguments.add(String.valueOf(limits.time.timeShift));
      }
    }

    // The executor expects a single IOResource.
    // However, we may have multiple IOResources due to using multiple cgroup groups.
    // We construct a single IOResource to account for this.
    return combineResources(resources);
  }

  private LinuxSandboxOptions decideLinuxSandboxOptions(
      ResourceLimits limits, Path workingDirectory) {
    // Construct the CLI options for this binary.
    LinuxSandboxOptions options = new LinuxSandboxOptions();
    options.createNetns = limits.network.blockNetwork;
    options.workingDir = workingDirectory.toString();

    // Bazel encodes these directly
    options.writableFiles.add(execFileSystem.root().toString());
    options.writableFiles.add(workingDirectory.toString());

    // For the time being, the linux-sandbox version of "nobody"
    // does not pair with buildfarm's implementation of exec_owner: "nobody".
    // This will need fixed to enable using fakeUsername with the sandbox.
    // TODO: provide proper support for bazel sandbox's fakeUsername "-U" flag.
    // options.fakeUsername = limits.fakeUsername;

    // these were hardcoded in bazel based on a filesystem configuration typical to ours
    // TODO: they may be incorrect for say Windows, and support will need adjusted in the future.
    options.writableFiles.add("/tmp");
    options.writableFiles.add("/dev/shm");

    if (limits.tmpFs) {
      options.tmpfsDirs.add("/tmp");
    }

    if (limits.debugAfterExecution) {
      options.statsPath = workingDirectory.resolve("action_execution_statistics").toString();
    }

    // Bazel looks through environment variables based on operation system to provide additional
    // write files.
    // TODO: Add other paths based on environment variables
    // all:     TEST_TMPDIR
    // windows: TEMP
    // windows: TMP
    // linux:   TMPDIR

    return options;
  }

  private void addLinuxSandboxCli(
      ImmutableList.Builder<String> arguments, LinuxSandboxOptions options) {
    arguments.add(ExecutionWrappers.AS_NOBODY);

    // Choose the sandbox which is built and deployed with the worker image.
    arguments.add(ExecutionWrappers.LINUX_SANDBOX);

    // Pass flags based on the sandbox CLI options.
    if (options.createNetns) {
      arguments.add("-N");
    }

    if (options.fakeUsername) {
      arguments.add("-U");
    }

    if (!options.workingDir.isEmpty()) {
      arguments.add("-W");
      arguments.add(options.workingDir);
    }
    if (!options.statsPath.isEmpty()) {
      arguments.add("-S");
      arguments.add(options.statsPath);
    }
    for (String writablePath : options.writableFiles) {
      arguments.add("-w");
      arguments.add(writablePath);
    }

    for (String dir : options.tmpfsDirs) {
      arguments.add("-e");
      arguments.add(dir);
    }

    arguments.add("--");
  }

  private void applyCpuLimits(Group group, ResourceLimits limits, ArrayList<IOResource> resources) {
    Cpu cpu = group.getCpu();
    try {
      cpu.close();
      if (limits.cpu.max > 0) {
        /* period of 100ms */
        cpu.setCFSPeriod(100000);
        cpu.setCFSQuota(limits.cpu.max * 100000);
      }
      if (limits.cpu.min > 0) {
        cpu.setShares(limits.cpu.min * 1024);
      }
    } catch (IOException e) {
      // clear interrupt flag if set due to ClosedByInterruptException
      boolean wasInterrupted = Thread.interrupted();
      try {
        cpu.close();
      } catch (IOException closeEx) {
        e.addSuppressed(closeEx);
      }
      if (wasInterrupted) {
        Thread.currentThread().interrupt();
      }
      throw new RuntimeException(e);
    }
    resources.add(cpu);
  }

  private void applyMemLimits(Group group, ResourceLimits limits, ArrayList<IOResource> resources) {
    try {
      Mem mem = group.getMem();
      mem.setMemoryLimit(limits.mem.claimed);
      resources.add(mem);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private IOResource combineResources(ArrayList<IOResource> resources) {
    return new IOResource() {
      @Override
      public void close() {
        for (IOResource resource : resources) {
          try {
            resource.close();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }

      @Override
      public boolean isReferenced() {
        for (IOResource resource : resources) {
          if (resource.isReferenced()) {
            return true;
          }
        }
        return false;
      }
    };
  }
}
