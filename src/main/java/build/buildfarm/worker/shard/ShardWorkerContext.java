// Copyright 2018 The Buildfarm Authors. All rights reserved.
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
import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.ExecutionStage;
import build.bazel.remote.execution.v2.Platform;
import build.buildfarm.backplane.Backplane;
import build.buildfarm.common.Claim;
import build.buildfarm.common.CommandUtils;
import build.buildfarm.common.DigestPath;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.EntryLimitException;
import build.buildfarm.common.ExecutionProperties;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.common.LinuxSandboxOptions;
import build.buildfarm.common.Poller;
import build.buildfarm.common.ProtoUtils;
import build.buildfarm.common.Size;
import build.buildfarm.common.SystemProcessors;
import build.buildfarm.common.Write;
import build.buildfarm.common.config.BuildfarmConfigs;
import build.buildfarm.common.config.ExecutionPolicy;
import build.buildfarm.common.function.IOConsumer;
import build.buildfarm.common.grpc.Retrier;
import build.buildfarm.common.grpc.Retrier.Backoff;
import build.buildfarm.instance.Instance;
import build.buildfarm.v1test.Digest;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.QueuedOperation;
import build.buildfarm.worker.DequeueMatchEvaluator;
import build.buildfarm.worker.ExecFileSystem;
import build.buildfarm.worker.ExecutionPolicies;
import build.buildfarm.worker.MatchListener;
import build.buildfarm.worker.RetryingMatchListener;
import build.buildfarm.worker.WorkerContext;
import build.buildfarm.worker.cgroup.CGroupVersion;
import build.buildfarm.worker.cgroup.Cpu;
import build.buildfarm.worker.cgroup.Group;
import build.buildfarm.worker.cgroup.Mem;
import build.buildfarm.worker.resources.LocalResourceSet;
import build.buildfarm.worker.resources.ResourceDecider;
import build.buildfarm.worker.resources.ResourceLimits;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.longrunning.Operation;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;
import com.google.rpc.PreconditionFailure;
import io.grpc.Deadline;
import io.grpc.Status;
import io.grpc.StatusException;
import io.prometheus.client.Counter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.UserPrincipal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import javax.annotation.Nullable;
import lombok.extern.java.Log;

@Log
class ShardWorkerContext implements WorkerContext {
  static final String EXEC_OWNER_RESOURCE_NAME = "exec-owner";
  private static final Platform.Property EXEC_OWNER_PROPERTY =
      Platform.Property.newBuilder().setName(EXEC_OWNER_RESOURCE_NAME).setValue("1").build();
  private static final String PROVISION_CORES_NAME = "cores";

  private static final Counter completedOperations =
      Counter.build().name("completed_operations").help("Completed operations.").register();
  private static final Counter operationPollerCounter =
      Counter.build().name("operation_poller").help("Number of operations polled.").register();

  private static BuildfarmConfigs configs = BuildfarmConfigs.getInstance();

  private final String name;
  private final SetMultimap<String, String> matchProvisions;
  private final Duration operationPollPeriod;
  private final OperationPoller operationPoller;
  private final int inputFetchDeadline;
  private final int inputFetchStageWidth;
  private final int executeStageWidth;
  private final int reportResultStageWidth;
  private final Backplane backplane;
  private final ExecFileSystem execFileSystem;
  private final InputStreamFactory inputStreamFactory;
  private final ListMultimap<String, ExecutionPolicy> policies;
  private final Instance instance;
  private final Duration defaultActionTimeout;
  private final Duration maximumActionTimeout;
  private final int defaultMaxCores;
  private final boolean limitGlobalExecution;
  private final boolean onlyMulticoreTests;
  private final boolean allowBringYourOwnContainer;
  private final Map<String, QueueEntry> activeOperations = Maps.newConcurrentMap();
  private final Group executionsGroup = Group.getRoot().getChild("executions");
  private final Group operationsGroup = executionsGroup.getChild("operations");
  private final CasWriter writer;
  private final boolean errorOperationRemainingResources;
  private final LocalResourceSet resourceSet;
  private final boolean errorOperationOutputSizeExceeded;
  private final boolean provideOwnedClaim;

  static SetMultimap<String, String> getMatchProvisions(
      Iterable<ExecutionPolicy> policies, String name, int executeStageWidth) {
    ImmutableSetMultimap.Builder<String, String> provisions = ImmutableSetMultimap.builder();
    Platform matchPlatform =
        ExecutionPolicies.getMatchPlatform(
            configs.getWorker().getDequeueMatchSettings().getPlatform(), policies);
    for (Platform.Property property : matchPlatform.getPropertiesList()) {
      provisions.put(property.getName(), property.getValue());
    }
    provisions.put(PROVISION_CORES_NAME, String.format("%d", executeStageWidth));
    provisions.put(ExecutionProperties.WORKER, name);
    return provisions.build();
  }

  ShardWorkerContext(
      String name,
      Duration operationPollPeriod,
      OperationPoller operationPoller,
      int inputFetchStageWidth,
      int executeStageWidth,
      int reportResultStageWidth,
      int inputFetchDeadline,
      Backplane backplane,
      ExecFileSystem execFileSystem,
      InputStreamFactory inputStreamFactory,
      Iterable<ExecutionPolicy> policies,
      Instance instance,
      Duration defaultActionTimeout,
      Duration maximumActionTimeout,
      int defaultMaxCores,
      boolean limitGlobalExecution,
      boolean onlyMulticoreTests,
      boolean allowBringYourOwnContainer,
      boolean errorOperationRemainingResources,
      boolean errorOperationOutputSizeExceeded,
      LocalResourceSet resourceSet,
      CasWriter writer) {
    this.name = name;
    this.matchProvisions = getMatchProvisions(policies, name, executeStageWidth);
    this.operationPollPeriod = operationPollPeriod;
    this.operationPoller = operationPoller;
    this.inputFetchStageWidth = inputFetchStageWidth;
    this.executeStageWidth = executeStageWidth;
    this.reportResultStageWidth = reportResultStageWidth;
    this.inputFetchDeadline = inputFetchDeadline;
    this.backplane = backplane;
    this.execFileSystem = execFileSystem;
    this.inputStreamFactory = inputStreamFactory;
    this.policies = ExecutionPolicies.toMultimap(policies);
    this.instance = instance;
    this.defaultActionTimeout = defaultActionTimeout;
    this.maximumActionTimeout = maximumActionTimeout;
    this.defaultMaxCores = defaultMaxCores;
    this.limitGlobalExecution = limitGlobalExecution;
    this.onlyMulticoreTests = onlyMulticoreTests;
    this.allowBringYourOwnContainer = allowBringYourOwnContainer;
    this.errorOperationRemainingResources = errorOperationRemainingResources;
    this.errorOperationOutputSizeExceeded = errorOperationOutputSizeExceeded;
    this.resourceSet = resourceSet;
    this.writer = writer;

    provideOwnedClaim = this.resourceSet.poolResources.containsKey(EXEC_OWNER_RESOURCE_NAME);
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
  public Poller createPoller(
      String name, QueueEntry queueEntry, ExecutionStage.Value stage, Executor executor) {
    Poller poller = new Poller(operationPollPeriod);
    resumePoller(poller, name, queueEntry, stage, () -> {}, Deadline.after(10, DAYS), executor);
    return poller;
  }

  @Override
  public void resumePoller(
      Poller poller,
      String name,
      QueueEntry queueEntry,
      ExecutionStage.Value stage,
      Runnable onFailure,
      Deadline deadline,
      Executor executor) {
    String operationName = queueEntry.getExecuteEntry().getOperationName();
    poller.resume(
        () -> {
          boolean success = false;
          try {
            success =
                operationPoller.poll(queueEntry, stage, System.currentTimeMillis() + 30 * 1000);
          } catch (IOException e) {
            log.log(
                Level.SEVERE, format("%s: poller: error while polling %s", name, operationName), e);
          }
          if (!success) {
            log.log(
                Level.WARNING,
                format("%s: poller: Completed Poll for %s: Failed", name, operationName));
            onFailure.run();
          } else {
            operationPollerCounter.inc();
            log.log(
                Level.FINEST, format("%s: poller: Completed Poll for %s: OK", name, operationName));
          }
          return success;
        },
        () -> {
          log.log(
              Level.WARNING, format("%s: poller: Deadline expired for %s", name, operationName));
          onFailure.run();
        },
        deadline,
        executor);
  }

  private ByteString getBlob(Digest digest) throws IOException, InterruptedException {
    try (InputStream in = inputStreamFactory.newInput(Compressor.Value.IDENTITY, digest, 0)) {
      return ByteString.readFrom(in);
    } catch (NoSuchFileException e) {
      return null;
    }
  }

  @Override
  public QueuedOperation getQueuedOperation(QueueEntry queueEntry)
      throws IOException, InterruptedException {
    ByteString queuedOperationBlob = getBlob(queueEntry.getQueuedOperationDigest());
    return ProtoUtils.parseQueuedOperation(queuedOperationBlob, queueEntry);
  }

  // FIXME make OwnedClaim with owner
  // how will this play out with persistent workers, should we have one per user?
  private @Nullable Claim acquireClaim(Platform platform) {
    // expand platform requirements with exec owner
    if (provideOwnedClaim) {
      platform = platform.toBuilder().addProperties(EXEC_OWNER_PROPERTY).build();
    }

    Claim claim = DequeueMatchEvaluator.acquireClaim(matchProvisions, resourceSet, platform);

    // a little awkward wrapping with the early return here to preserve effective final
    if (claim != null && provideOwnedClaim) {
      // enforced by exec owner property value of "1"
      for (Entry<String, List<Object>> pool : claim.getPools()) {
        if (!pool.getKey().equals(EXEC_OWNER_RESOURCE_NAME)) {
          continue;
        }
        String name = (String) Iterables.getOnlyElement(pool.getValue());

        return new Claim() {
          UserPrincipal owner = execFileSystem.getOwner(name);

          @Override
          public void release(Claim.Stage stage) {
            claim.release(stage);
          }

          @Override
          public void release() {
            owner = null;
            claim.release();
          }

          @Override
          public UserPrincipal owner() {
            if (owner != null) {
              return owner;
            }
            return claim.owner();
          }

          @Override
          public Iterable<Entry<String, List<Object>>> getPools() {
            return claim.getPools();
          }
        };
      }
      // claim was not provided with owner name value
    }
    return claim;
  }

  @SuppressWarnings("ConstantConditions")
  private void matchInterruptible(MatchListener listener) throws IOException, InterruptedException {
    QueueEntry queueEntry = takeEntryOffOperationQueue(listener);
    Claim claim = null;
    if (queueEntry == null || (claim = acquireClaim(queueEntry.getPlatform())) != null) {
      listener.onEntry(queueEntry, claim);
    } else {
      backplane.rejectOperation(queueEntry);
    }
  }

  private @Nullable QueueEntry takeEntryOffOperationQueue(MatchListener listener)
      throws IOException, InterruptedException {
    if (!listener.onWaitStart()) {
      // match listener can signal completion here
      return null;
    }
    QueueEntry queueEntry = null;
    try {
      queueEntry =
          backplane.dispatchOperation(
              configs.getWorker().getDequeueMatchSettings().getPlatform().getPropertiesList(),
              resourceSet);
    } catch (IOException e) {
      Status status = Status.fromThrowable(e);
      switch (status.getCode()) {
        case DEADLINE_EXCEEDED:
          log.log(Level.WARNING, "backplane timed out for match during bookkeeping");
          break;
        case UNAVAILABLE:
          log.log(Level.WARNING, "backplane was unavailable for match");
          break;
        default:
          throw e;
      }
      // transient backplane errors will propagate a null queueEntry
    }
    listener.onWaitEnd();
    return queueEntry;
  }

  @Override
  public void match(MatchListener listener) throws InterruptedException {
    RetryingMatchListener dedupMatchListener =
        new RetryingMatchListener() {
          boolean matched = false;

          @Override
          public boolean isMatched() {
            return matched;
          }

          @Override
          public boolean onWaitStart() {
            return listener.onWaitStart();
          }

          @Override
          public void onWaitEnd() {
            listener.onWaitEnd();
          }

          @Override
          public boolean onEntry(@Nullable QueueEntry queueEntry, Claim claim)
              throws InterruptedException {
            if (queueEntry == null) {
              matched = true;
              return listener.onEntry(null, null);
            }
            return onValidEntry(queueEntry, claim);
          }

          private boolean onValidEntry(QueueEntry queueEntry, Claim claim)
              throws InterruptedException {
            String operationName = queueEntry.getExecuteEntry().getOperationName();
            if (activeOperations.putIfAbsent(operationName, queueEntry) != null) {
              claim.release();
              log.log(Level.WARNING, "matched duplicate operation " + operationName);
              return false;
            }
            return onUniqueEntry(queueEntry, claim);
          }

          private boolean onUniqueEntry(QueueEntry queueEntry, Claim claim)
              throws InterruptedException {
            matched = true;
            boolean success = listener.onEntry(queueEntry, claim);
            if (!success) {
              claim.release();
              requeue(queueEntry.getExecuteEntry().getOperationName());
            }
            return success;
          }

          @Override
          public void onError(Throwable t) {
            Throwables.throwIfUnchecked(t);
            throw new RuntimeException(t);
          }
        };
    while (!dedupMatchListener.isMatched()) {
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
      log.log(Level.SEVERE, "Failure while trying to fast requeue " + operationName, e);
    }
  }

  void requeue(Operation operation) {
    requeue(operation.getName());
  }

  void deactivate(String operationName) {
    activeOperations.remove(operationName);
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
  public int getReportResultStageWidth() {
    return reportResultStageWidth;
  }

  @Override
  public boolean hasDefaultActionTimeout() {
    return Durations.isPositive(defaultActionTimeout);
  }

  @Override
  public boolean hasMaximumActionTimeout() {
    return Durations.isPositive(maximumActionTimeout);
  }

  @Override
  public boolean isStreamStdout() {
    return true;
  }

  @Override
  public boolean isStreamStderr() {
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
    if (digest.getSize() > 0) {
      writer.insertBlob(digest, content);
    }
  }

  private void insertFile(Digest digest, Path file) throws IOException, InterruptedException {
    writer.write(digest, file);
  }

  private void updateActionResultStdOutputs(
      ActionResult.Builder resultBuilder, DigestUtil digestUtil)
      throws IOException, InterruptedException {
    ByteString stdoutRaw = resultBuilder.getStdoutRaw();
    if (stdoutRaw.size() > 0) {
      // reset to allow policy to determine inlining
      resultBuilder.setStdoutRaw(ByteString.EMPTY);
      Digest stdoutDigest = digestUtil.compute(stdoutRaw);
      insertBlob(stdoutDigest, stdoutRaw);
      resultBuilder.setStdoutDigest(DigestUtil.toDigest(stdoutDigest));
    }

    ByteString stderrRaw = resultBuilder.getStderrRaw();
    if (stderrRaw.size() > 0) {
      // reset to allow policy to determine inlining
      resultBuilder.setStderrRaw(ByteString.EMPTY);
      Digest stderrDigest = digestUtil.compute(stderrRaw);
      insertBlob(stderrDigest, stderrRaw);
      resultBuilder.setStderrDigest(DigestUtil.toDigest(stderrDigest));
    }
  }

  private static String toREOutputPath(String nativePath) {
    // RE API OutputFile/Directory path
    // The path separator is a forward slash `/`.
    if (File.separatorChar != '/') {
      return nativePath.replace(File.separatorChar, '/');
    }
    return nativePath;
  }

  private void uploadOutputFile(
      ActionResult.Builder resultBuilder,
      DigestUtil digestUtil,
      Path outputPath,
      Path workingDirectory,
      String entrySizeViolationType,
      PreconditionFailure.Builder preconditionFailure)
      throws IOException, InterruptedException {
    String outputFile = toREOutputPath(workingDirectory.relativize(outputPath).toString());

    if (!Files.exists(outputPath)) {
      log.log(Level.FINER, "ReportResultStage: " + outputFile + " does not exist...");
      return;
    }

    if (Files.isDirectory(outputPath)) {
      String message =
          String.format(
              "ReportResultStage: %s is a directory but it should have been a file", outputPath);
      log.log(Level.FINER, message);
      preconditionFailure
          .addViolationsBuilder()
          .setType(VIOLATION_TYPE_INVALID)
          .setSubject(outputFile)
          .setDescription(message);
      return;
    }

    long size = Files.size(outputPath);
    long maxEntrySize = execFileSystem.getStorage().maxEntrySize();
    if (maxEntrySize != UNLIMITED_ENTRY_SIZE_MAX && size > maxEntrySize) {
      String message =
          String.format(
              "ReportResultStage: The output %s could not be uploaded because it exceeded the"
                  + " maximum size of an entry (%d > %d)",
              outputPath, size, maxEntrySize);
      preconditionFailure
          .addViolationsBuilder()
          .setType(entrySizeViolationType)
          .setSubject(outputFile + ": " + size)
          .setDescription(message);
      return;
    }

    // will run into issues if we end up blocking on the cache insertion, might
    // want to decrement input references *before* this to ensure that we cannot
    // cause an internal deadlock

    Digest digest;
    try {
      digest = digestUtil.compute(outputPath);
    } catch (NoSuchFileException e) {
      return;
    }

    resultBuilder
        .addOutputFilesBuilder()
        .setPath(outputFile)
        .setDigest(DigestUtil.toDigest(digest))
        .setIsExecutable(Files.isExecutable(outputPath));

    try {
      insertFile(digest, outputPath);
    } catch (EntryLimitException e) {
      preconditionFailure
          .addViolationsBuilder()
          .setType(entrySizeViolationType)
          .setSubject("blobs/" + DigestUtil.toString(digest))
          .setDescription(
              "An output could not be uploaded because it exceeded the maximum size of an entry");
    }
  }

  private void uploadOutputDirectory(
      ActionResult.Builder resultBuilder,
      DigestUtil digestUtil,
      Path outputDirPath,
      Path workingDirectory,
      String entrySizeViolationType,
      PreconditionFailure.Builder preconditionFailure)
      throws IOException, InterruptedException {
    String outputDir = toREOutputPath(workingDirectory.relativize(outputDirPath).toString());

    if (!Files.exists(outputDirPath)) {
      log.log(Level.FINER, "ReportResultStage: " + outputDir + " does not exist...");
      return;
    }

    if (!Files.isDirectory(outputDirPath)) {
      log.log(Level.FINER, "ReportResultStage: " + outputDir + " is not a directory...");
      preconditionFailure
          .addViolationsBuilder()
          .setType(VIOLATION_TYPE_INVALID)
          .setSubject(outputDir)
          .setDescription("An output directory was not a directory");
      return;
    }

    IOConsumer<DigestPath> fileObserver =
        digestPath -> {
          Digest digest = digestPath.digest();
          try {
            insertFile(digest, digestPath.path());
          } catch (InterruptedException e) {
            throw new IOException(e);
          } catch (EntryLimitException e) {
            preconditionFailure
                .addViolationsBuilder()
                .setType(entrySizeViolationType)
                .setSubject("blobs/" + DigestUtil.toString(digest))
                .setDescription(
                    "An output could not be uploaded because it exceeded "
                        + "the maximum size of an entry");
          }
        };
    TreeWalker treeWalker =
        new TreeWalker(configs.getWorker().isCreateSymlinkOutputs(), digestUtil, fileObserver);
    Files.walkFileTree(outputDirPath, treeWalker);
    ByteString treeBlob = treeWalker.getTree().toByteString();
    Digest treeDigest = digestUtil.compute(treeBlob);
    insertBlob(treeDigest, treeBlob);
    resultBuilder
        .addOutputDirectoriesBuilder()
        .setPath(outputDir)
        .setTreeDigest(DigestUtil.toDigest(treeDigest));
  }

  @Override
  public void uploadOutputs(
      Digest actionDigest, ActionResult.Builder resultBuilder, Path actionRoot, Command command)
      throws IOException, InterruptedException, StatusException {
    String entrySizeViolationType =
        errorOperationOutputSizeExceeded ? VIOLATION_TYPE_INVALID : VIOLATION_TYPE_MISSING;

    PreconditionFailure.Builder preconditionFailure = PreconditionFailure.newBuilder();

    Path workingDirectory = actionRoot.resolve(command.getWorkingDirectory());
    List<Path> outputPaths = CommandUtils.getResolvedOutputPaths(command, workingDirectory);
    DigestUtil digestUtil = new DigestUtil(HashFunction.get(actionDigest.getDigestFunction()));
    for (Path outputPath : outputPaths) {
      if (Files.isDirectory(outputPath)) {
        uploadOutputDirectory(
            resultBuilder,
            digestUtil,
            outputPath,
            workingDirectory,
            entrySizeViolationType,
            preconditionFailure);
      } else {
        uploadOutputFile(
            resultBuilder,
            digestUtil,
            outputPath,
            workingDirectory,
            entrySizeViolationType,
            preconditionFailure);
      }
    }
    checkPreconditionFailure(actionDigest, preconditionFailure.build());

    /* put together our outputs and update the result */
    updateActionResultStdOutputs(resultBuilder, digestUtil);
  }

  @Override
  public List<ExecutionPolicy> getExecutionPolicies(String name) {
    return policies.get(name);
  }

  @Override
  public void unmergeExecution(ActionKey actionKey) throws IOException, InterruptedException {
    createBackplaneRetrier()
        .execute(
            () -> {
              backplane.unmergeExecution(actionKey);
              return null;
            });
  }

  @Override
  public boolean putOperation(Operation operation) throws IOException, InterruptedException {
    boolean success = createBackplaneRetrier().execute(() -> instance.putOperation(operation));
    if (success && operation.getDone()) {
      completedOperations.inc();
      log.log(Level.FINER, "CompletedOperation: " + operation.getName());
    }
    return success;
  }

  @Override
  public Path createExecDir(
      String operationName,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      DigestFunction.Value digestFunction,
      Action action,
      Command command,
      @Nullable UserPrincipal owner)
      throws IOException, InterruptedException {
    return execFileSystem.createExecDir(
        operationName, directoriesIndex, digestFunction, action, command, owner);
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

  boolean shouldLimitCoreUsage() {
    return limitGlobalExecution || onlyMulticoreTests || defaultMaxCores > 0;
  }

  @Override
  public void createExecutionLimits() {
    if (shouldLimitCoreUsage() && configs.getWorker().getSandboxSettings().isAlwaysUseCgroups()) {
      createOperationExecutionLimits();
    }
  }

  void createOperationExecutionLimits() {
    try {
      int availableProcessors = SystemProcessors.get();
      Preconditions.checkState(availableProcessors >= executeStageWidth);
      executionsGroup.getCpu().setMaxCpu(executeStageWidth);
      if (executeStageWidth < availableProcessors) {
        /* only divide up our cfs quota if we need to limit below the available processors for executions */
        executionsGroup.getCpu().setMaxCpu(executeStageWidth);
      }
      // create `execution width` shares to choose from. This is the ceiling for the operations
      operationsGroup.getCpu().setShares(executeStageWidth);
    } catch (IOException | IllegalStateException e) {
      log.log(Level.WARNING, "Unable to set up CGroup", e);
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
    if (configs.getWorker().getSandboxSettings().isAlwaysUseCgroups()) {
      try {
        operationsGroup.getCpu().close();
        executionsGroup.getCpu().close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  String getOperationId(String operationName) {
    String[] components = operationName.split("/");
    Preconditions.checkState(components.length >= 3);
    String binding = components[components.length - 2];
    // legacy
    Preconditions.checkState(binding.equals("operations") || binding.equals("executions"));
    return components[components.length - 1];
  }

  @Override
  public int commandExecutionClaims(Command command) {
    return commandExecutionSettings(command).cpu.claimed;
  }

  public ResourceLimits commandExecutionSettings(Command command) {
    return ResourceDecider.decideResourceLimitations(
        command,
        name,
        defaultMaxCores,
        onlyMulticoreTests,
        limitGlobalExecution,
        getExecuteStageWidth(),
        allowBringYourOwnContainer,
        configs.getWorker().getSandboxSettings());
  }

  // This has become an extremely awkward mechanism
  // an exec dir should be associatable with all of these parameters
  // and the arguments should be more structured and less susceptible to ordering details
  @Override
  public IOResource limitExecution(
      String operationName,
      @Nullable UserPrincipal owner,
      ImmutableList.Builder<String> arguments,
      Command command,
      Path workingDirectory) {
    ResourceLimits limits = commandExecutionSettings(command);
    IOResource resource;
    if (shouldLimitCoreUsage()) {
      resource = limitSpecifiedExecution(limits, operationName, owner, arguments, workingDirectory);
    } else {
      resource =
          new IOResource() {
            @Override
            public void close() {}

            @Override
            public boolean isReferenced() {
              return false;
            }
          };
    }

    // Possibly set network restrictions.
    // This is not the ideal implementation of block-network.
    // For now, without the linux-sandbox, we will unshare the network namespace.
    if (limits.network.blockNetwork && !limits.useLinuxSandbox) {
      arguments.add(configs.getExecutionWrappers().getUnshare(), "-n", "-r");
    }

    // Decide the CLI for running the sandbox
    // For reference on how bazel spawns the sandbox:
    // https://github.com/bazelbuild/bazel/blob/ddf302e2798be28bb67e32d5c2fc9c73a6a1fbf4/src/main/java/com/google/devtools/build/lib/sandbox/LinuxSandboxUtil.java#L183
    if (limits.useLinuxSandbox) {
      LinuxSandboxOptions options = decideLinuxSandboxOptions(limits, workingDirectory);
      addLinuxSandboxCli(arguments, options);
    }

    if (configs.getWorker().getSandboxSettings().isAlwaysUseAsNobody() || limits.fakeUsername) {
      arguments.add(configs.getExecutionWrappers().getAsNobody());
    }

    if (limits.time.skipSleep) {
      arguments.add(configs.getExecutionWrappers().getSkipSleep());

      // we set these values very high because we want sleep calls to return immediately.
      arguments.add("90000000"); // delay factor
      arguments.add("90000000"); // time factor
      arguments.add(configs.getExecutionWrappers().getSkipSleepPreload());

      if (limits.time.timeShift != 0) {
        arguments.add(configs.getExecutionWrappers().getDelay());
        arguments.add(String.valueOf(limits.time.timeShift));
      }
    }
    return resource;
  }

  private String getCgroups() {
    if (Group.VERSION == CGroupVersion.CGROUPS_V2) {
      return configs.getExecutionWrappers().getCgroups2();
    }
    return configs.getExecutionWrappers().getCgroups1();
  }

  IOResource limitSpecifiedExecution(
      ResourceLimits limits,
      String operationName,
      @Nullable UserPrincipal owner,
      ImmutableList.Builder<String> arguments,
      Path workingDirectory) {
    // The decision to apply resource restrictions has already been decided within the
    // ResourceLimits object. We apply the cgroup settings to file resources
    // and collect group names to use on the CLI.
    String operationId = getOperationId(operationName);
    ArrayList<IOResource> resources = new ArrayList<>();
    if (limits.cgroups) {
      final Group group = operationsGroup.getChild(operationId);
      ArrayList<String> usedGroups = new ArrayList<>();

      // Possibly set core restrictions.
      if (limits.cpu.limit) {
        log.log(Level.FINEST, "Applying CPU limit {0}", limits.cpu);
        applyCpuLimits(group, owner, limits, resources);
        usedGroups.add(group.getCpu().getControllerName());
      }

      // Possibly set memory restrictions.
      if (limits.mem.limit) {
        log.log(Level.FINEST, "Applying Mem limit {0}", limits.mem);
        applyMemLimits(group, owner, limits, resources);
        usedGroups.add(group.getMem().getControllerName());
      }

      // Decide the CLI for running under cgroups
      if (!usedGroups.isEmpty()) {
        arguments.add(
            getCgroups(), "-g", String.join(",", usedGroups) + ":" + group.getHierarchy());
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
    options.fakeHostname = limits.network.fakeHostname;
    options.workingDir = workingDirectory.toString();

    // Bazel encodes these directly
    options.writableFiles.add(execFileSystem.root().toString());
    options.writableFiles.add(workingDirectory.toString());

    // For the time being, the linux-sandbox version of "nobody"
    // does not pair with buildfarm's implementation of exec_owner: "nobody".
    // This will need fixed to enable using fakeUsername with the sandbox.
    // TODO: provide proper support for bazel sandbox's fakeUsername "-U" flag.
    // options.fakeUsername = limits.fakeUsername;

    options.writableFiles.addAll(
        configs.getWorker().getSandboxSettings().getAdditionalWritePaths());

    if (limits.tmpFs) {
      options.tmpfsDirs.addAll(configs.getWorker().getSandboxSettings().getTmpFsPaths());
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
    // Choose the sandbox which is built and deployed with the worker image.
    arguments.add(configs.getExecutionWrappers().getLinuxSandbox());

    // Pass flags based on the sandbox CLI options.
    if (options.createNetns) {
      arguments.add("-N");
    }

    if (options.fakeHostname) {
      arguments.add("-H");
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

  private void applyCpuLimits(
      Group group,
      @Nullable UserPrincipal owner,
      ResourceLimits limits,
      ArrayList<IOResource> resources) {
    Cpu cpu = group.getCpu();
    try {
      cpu.close();

      if (owner != null) {
        // Associate cgroup ownership
        cpu.setOwner(owner);
      }

      if (limits.cpu.max > 0) {
        cpu.setMaxCpu(limits.cpu.max);
      }
      if (limits.cpu.min > 0) {
        cpu.setShares(limits.cpu.min);
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

  private void applyMemLimits(
      Group group,
      @Nullable UserPrincipal owner,
      ResourceLimits limits,
      ArrayList<IOResource> resources) {
    try {
      Mem mem = group.getMem();
      if (owner != null) {
        // Associate cgroup ownership
        mem.setOwner(owner);
      }

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
