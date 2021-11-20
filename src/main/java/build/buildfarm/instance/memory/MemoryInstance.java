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

package build.buildfarm.instance.memory;

import static build.buildfarm.common.Actions.invalidActionVerboseMessage;
import static build.buildfarm.common.Errors.VIOLATION_TYPE_INVALID;
import static build.buildfarm.common.Errors.VIOLATION_TYPE_MISSING;
import static build.buildfarm.instance.Utils.putBlob;
import static com.google.common.collect.Multimaps.synchronizedSetMultimap;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.lang.String.format;
import static java.util.Collections.synchronizedSortedMap;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecutionStage;
import build.bazel.remote.execution.v2.Platform;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.ac.ActionCache;
import build.buildfarm.ac.FilesystemActionCache;
import build.buildfarm.ac.GrpcActionCache;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.cas.ContentAddressableStorages;
import build.buildfarm.common.CasIndexResults;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.TokenizableIterator;
import build.buildfarm.common.TreeIterator;
import build.buildfarm.common.TreeIterator.DirectoryEntry;
import build.buildfarm.common.Watchdog;
import build.buildfarm.common.Watcher;
import build.buildfarm.common.Write;
import build.buildfarm.common.io.FeedbackOutputStream;
import build.buildfarm.instance.MatchListener;
import build.buildfarm.instance.queues.Worker;
import build.buildfarm.instance.queues.WorkerQueue;
import build.buildfarm.instance.queues.WorkerQueueConfigurations;
import build.buildfarm.instance.queues.WorkerQueues;
import build.buildfarm.instance.server.AbstractServerInstance;
import build.buildfarm.instance.server.OperationsMap;
import build.buildfarm.instance.server.WatchFuture;
import build.buildfarm.operations.FindOperationsResults;
import build.buildfarm.v1test.ActionCacheConfig;
import build.buildfarm.v1test.BackplaneStatus;
import build.buildfarm.v1test.ExecuteEntry;
import build.buildfarm.v1test.FilesystemACConfig;
import build.buildfarm.v1test.GetClientStartTimeRequest;
import build.buildfarm.v1test.GetClientStartTimeResult;
import build.buildfarm.v1test.GrpcACConfig;
import build.buildfarm.v1test.MemoryInstanceConfig;
import build.buildfarm.v1test.OperationIteratorToken;
import build.buildfarm.v1test.OperationQueueStatus;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.QueuedOperation;
import build.buildfarm.v1test.Tree;
import build.buildfarm.worker.DequeueMatchEvaluator;
import build.buildfarm.worker.DequeueMatchSettings;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import com.google.rpc.PreconditionFailure;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import javax.naming.ConfigurationException;

public class MemoryInstance extends AbstractServerInstance {
  private static final Logger logger = Logger.getLogger(MemoryInstance.class.getName());

  public static final String TIMEOUT_OUT_OF_BOUNDS =
      "A timeout specified is out of bounds with a configured range";

  private final MemoryInstanceConfig config;
  private final SetMultimap<String, WatchFuture> watchers;
  private final LoadingCache<String, ByteStringStreamSource> streams =
      CacheBuilder.newBuilder()
          .expireAfterWrite(1, TimeUnit.HOURS)
          .removalListener(
              (RemovalListener<String, ByteStringStreamSource>)
                  notification -> {
                    try {
                      notification.getValue().getOutput().close();
                    } catch (IOException e) {
                      logger.log(
                          Level.SEVERE,
                          format("error closing stream source %s", notification.getKey()),
                          e);
                    }
                  })
          .build(
              new CacheLoader<String, ByteStringStreamSource>() {
                @SuppressWarnings("NullableProblems")
                @Override
                public ByteStringStreamSource load(String name) {
                  return newStreamSource(name);
                }
              });

  private final WorkerQueues queuedOperations = WorkerQueueConfigurations.gpuAndFallback();

  private final Map<String, Watchdog> requeuers;
  private final Map<String, Watchdog> operationTimeoutDelays;
  private final OperationsMap outstandingOperations;
  private final Executor watcherExecutor;

  static SetMultimap<String, String> createProvisions(Platform platform) {
    ImmutableSetMultimap.Builder<String, String> provisions = ImmutableSetMultimap.builder();
    for (Platform.Property property : platform.getPropertiesList()) {
      provisions.put(property.getName(), property.getValue());
    }
    return provisions.build();
  }

  static class OutstandingOperations implements OperationsMap {
    private final Map<String, Operation> map = synchronizedSortedMap(new TreeMap<>());

    @Override
    public Operation remove(String name) {
      return map.remove(name);
    }

    @Override
    public boolean contains(String name) {
      return map.containsKey(name);
    }

    @Override
    public void put(String name, Operation operation) {
      map.put(name, operation);
    }

    @Override
    public Operation get(String name) {
      return map.get(name);
    }

    @Override
    public Iterator<Operation> iterator() {
      return map.values().iterator();
    }
  }

  public MemoryInstance(String name, DigestUtil digestUtil, MemoryInstanceConfig config)
      throws ConfigurationException {
    this(
        name,
        digestUtil,
        config,
        ContentAddressableStorages.create(config.getCasConfig()),
        /* watchers=*/ synchronizedSetMultimap(
            MultimapBuilder.hashKeys().hashSetValues(/* expectedValuesPerKey=*/ 1).build()),
        /* watcherExecutor=*/ newCachedThreadPool(),
        new OutstandingOperations(),
        /* workers=*/ Lists.newArrayList(),
        /* requeuers=*/ Maps.newConcurrentMap(),
        /* operationTimeoutDelays=*/ Maps.newConcurrentMap());
  }

  @VisibleForTesting
  public MemoryInstance(
      String name,
      DigestUtil digestUtil,
      MemoryInstanceConfig config,
      ContentAddressableStorage contentAddressableStorage,
      SetMultimap<String, WatchFuture> watchers,
      Executor watcherExecutor,
      OperationsMap outstandingOperations,
      List<Worker> workers,
      Map<String, Watchdog> requeuers,
      Map<String, Watchdog> operationTimeoutDelays) {
    super(
        name,
        digestUtil,
        contentAddressableStorage,
        MemoryInstance.createActionCache(
            config.getActionCacheConfig(), contentAddressableStorage, digestUtil),
        outstandingOperations,
        MemoryInstance.createCompletedOperationMap(contentAddressableStorage, digestUtil),
        /*activeBlobWrites=*/ new ConcurrentHashMap<>());
    this.config = config;
    this.watchers = watchers;
    this.outstandingOperations = outstandingOperations;
    this.watcherExecutor = watcherExecutor;
    this.requeuers = requeuers;
    this.operationTimeoutDelays = operationTimeoutDelays;

    // a default configuration for a GPU/fallback configuration
    queuedOperations.AddWorkers("Other", workers);
  }

  private static ActionCache createActionCache(
      ActionCacheConfig config, ContentAddressableStorage cas, DigestUtil digestUtil) {
    switch (config.getTypeCase()) {
      default:
      case TYPE_NOT_SET:
        throw new IllegalArgumentException("ActionCache config not set in config");
      case GRPC:
        return createGrpcActionCache(config.getGrpc());
      case DELEGATE_CAS:
        return createDelegateCASActionCache(cas, digestUtil);
      case FILESYSTEM:
        return createFilesystemActionCache(config.getFilesystem());
    }
  }

  private static Channel createChannel(String target) {
    NettyChannelBuilder builder =
        NettyChannelBuilder.forTarget(target).negotiationType(NegotiationType.PLAINTEXT);
    return builder.build();
  }

  private static ActionCache createGrpcActionCache(GrpcACConfig config) {
    Channel channel = createChannel(config.getTarget());
    return new GrpcActionCache(config.getInstanceName(), channel);
  }

  private static ActionCache createDelegateCASActionCache(
      ContentAddressableStorage cas, DigestUtil digestUtil) {
    return new ActionCache() {
      final DelegateCASMap<ActionKey, ActionResult> map =
          new DelegateCASMap<>(cas, ActionResult.parser(), digestUtil);

      @Override
      public ListenableFuture<ActionResult> get(ActionKey actionKey) {
        return immediateFuture(map.get(actionKey));
      }

      @Override
      public void put(ActionKey actionKey, ActionResult actionResult) throws InterruptedException {
        map.put(actionKey, actionResult);
      }
    };
  }

  private static ActionCache createFilesystemActionCache(FilesystemACConfig config) {
    return new FilesystemActionCache(Paths.get(config.getPath()));
  }

  private static OperationsMap createCompletedOperationMap(
      ContentAddressableStorage cas, DigestUtil digestUtil) {
    return new OperationsMap() {
      final DelegateCASMap<String, Operation> map =
          new DelegateCASMap<>(cas, Operation.parser(), digestUtil);

      @Override
      public Operation remove(String name) {
        return map.remove(name);
      }

      @Override
      public boolean contains(String name) {
        return map.containsKey(name);
      }

      @Override
      public void put(String name, Operation operation) throws InterruptedException {
        map.put(name, operation);
      }

      @Override
      public Operation get(String name) {
        return map.get(name);
      }

      @Override
      public Iterator<Operation> iterator() {
        throw new UnsupportedOperationException();
      }
    };
  }

  ByteStringStreamSource newStreamSource(String name) {
    ByteStringStreamSource source = new ByteStringStreamSource();
    source.getClosedFuture().addListener(() -> streams.invalidate(name), directExecutor());
    return source;
  }

  ByteStringStreamSource getStreamSource(String name) {
    try {
      return streams.get(name);
    } catch (ExecutionException e) {
      //noinspection deprecation
      Throwables.propagateIfInstanceOf(e.getCause(), RuntimeException.class);
      throw new UncheckedExecutionException(e.getCause());
    }
  }

  @Override
  public Write getOperationStreamWrite(String name) {
    return new Write() {
      @Override
      public long getCommittedSize() {
        return getStreamSource(name).getCommittedSize();
      }

      @Override
      public boolean isComplete() {
        return getStreamSource(name).isClosed();
      }

      @Override
      public FeedbackOutputStream getOutput(
          long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler) {
        // should be synchronized for a single active output
        return getStreamSource(name).getOutput();
      }

      @Override
      public ListenableFuture<FeedbackOutputStream> getOutputFuture(
          long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler) {
        // should be futured for a single closed output
        return immediateFuture(getOutput(deadlineAfter, deadlineAfterUnits, onReadyHandler));
      }

      @Override
      public void reset() {
        streams.invalidate(name);
      }

      @Override
      public ListenableFuture<Long> getFuture() {
        ByteStringStreamSource source = getStreamSource(name);
        return Futures.transform(
            source.getClosedFuture(), result -> source.getCommittedSize(), directExecutor());
      }
    };
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Override
  public InputStream newOperationStreamInput(
      String name, long offset, RequestMetadata requestMetadata) throws IOException {
    InputStream in = getStreamSource(name).openStream();
    in.skip(offset);
    return in;
  }

  @Override
  protected void enqueueOperation(Operation operation) {
    try {
      queuedOperations.enqueueOperation(operation, getOperationProvisions(operation));
    } catch (InterruptedException e) {
      logger.log(Level.SEVERE, format("failed to enqueueOperation: %s", operation), e);
    }
  }

  @Override
  protected void updateOperationWatchers(Operation operation) throws InterruptedException {
    super.updateOperationWatchers(operation);

    Set<WatchFuture> operationWatchers = watchers.get(operation.getName());
    synchronized (watchers) {
      for (WatchFuture watcher : operationWatchers) {
        watcherExecutor.execute(() -> watcher.observe(operation));
      }
    }
  }

  @Override
  protected Operation createOperation(ActionKey actionKey) {
    String name = createOperationName(UUID.randomUUID().toString());

    ExecuteOperationMetadata metadata =
        ExecuteOperationMetadata.newBuilder().setActionDigest(actionKey.getDigest()).build();

    return Operation.newBuilder()
        .setName(name)
        .setDone(false)
        .setMetadata(Any.pack(metadata))
        .build();
  }

  @Override
  protected void validateAction(
      String operationName,
      Action action,
      PreconditionFailure.Builder preconditionFailure,
      RequestMetadata requestMetadata)
      throws InterruptedException, StatusException {
    if (action.hasTimeout() && config.hasMaximumActionTimeout()) {
      Duration timeout = action.getTimeout();
      Duration maximum = config.getMaximumActionTimeout();
      if (timeout.getSeconds() > maximum.getSeconds()
          || (timeout.getSeconds() == maximum.getSeconds()
              && timeout.getNanos() > maximum.getNanos())) {
        preconditionFailure
            .addViolationsBuilder()
            .setType(VIOLATION_TYPE_INVALID)
            .setSubject(Durations.toString(timeout) + " > " + Durations.toString(maximum))
            .setDescription(TIMEOUT_OUT_OF_BOUNDS);
      }
    }

    super.validateAction(operationName, action, preconditionFailure, requestMetadata);
  }

  @Override
  public boolean pollOperation(String operationName, ExecutionStage.Value stage) {
    if (!super.pollOperation(operationName, stage)) {
      return false;
    }
    // pet the requeue watchdog
    Watchdog requeuer = requeuers.get(operationName);
    if (requeuer == null) {
      return false;
    }
    requeuer.pet();
    return true;
  }

  protected Action expectAction(Operation operation) throws InterruptedException {
    try {
      ExecuteOperationMetadata metadata = expectExecuteOperationMetadata(operation);
      if (metadata == null) {
        return null;
      }
      ByteString actionBlob = getBlob(metadata.getActionDigest());
      if (actionBlob != null) {
        return Action.parseFrom(actionBlob);
      }
    } catch (IOException e) {
      Status status = Status.fromThrowable(e);
      if (status.getCode() != io.grpc.Status.Code.NOT_FOUND) {
        logger.log(Level.SEVERE, "error retrieving action", e);
      }
    }
    return null;
  }

  @SuppressWarnings({"ProtoBuilderReturnValueIgnored", "ReturnValueIgnored"})
  private Action getActionForTimeoutMonitor(
      Operation operation, com.google.rpc.Status.Builder status) throws InterruptedException {
    Digest actionDigest = expectActionDigest(operation);
    if (actionDigest == null) {
      logger.log(
          Level.WARNING,
          format("Could not determine Action Digest for operation %s", operation.getName()));
      String message =
          String.format("Could not determine Action Digest from Operation %s", operation.getName());
      status.setCode(com.google.rpc.Code.INTERNAL.getNumber()).setMessage(message);
      return null;
    }
    ByteString actionBlob = getBlob(actionDigest);
    if (actionBlob == null) {
      logger.log(
          Level.WARNING,
          format(
              "Action %s for operation %s went missing, cannot initiate execution monitoring",
              DigestUtil.toString(actionDigest), operation.getName()));
      PreconditionFailure.Builder preconditionFailureBuilder = PreconditionFailure.newBuilder();
      preconditionFailureBuilder
          .addViolationsBuilder()
          .setType(VIOLATION_TYPE_MISSING)
          .setSubject("blobs/" + DigestUtil.toString(actionDigest))
          .setDescription(MISSING_ACTION);
      PreconditionFailure preconditionFailure = preconditionFailureBuilder.build();
      status
          .setCode(com.google.rpc.Code.FAILED_PRECONDITION.getNumber())
          .setMessage(invalidActionVerboseMessage(actionDigest, preconditionFailure))
          .addDetails(Any.pack(preconditionFailure))
          .build();
      return null;
    }
    try {
      return Action.parseFrom(actionBlob);
    } catch (InvalidProtocolBufferException e) {
      logger.log(
          Level.WARNING,
          format(
              "Could not parse Action %s for Operation %s",
              DigestUtil.toString(actionDigest), operation.getName()),
          e);
      PreconditionFailure.Builder preconditionFailureBuilder = PreconditionFailure.newBuilder();
      preconditionFailureBuilder
          .addViolationsBuilder()
          .setType(VIOLATION_TYPE_INVALID)
          .setSubject(INVALID_ACTION)
          .setDescription("Action " + DigestUtil.toString(actionDigest));
      PreconditionFailure preconditionFailure = preconditionFailureBuilder.build();
      status
          .setCode(com.google.rpc.Code.FAILED_PRECONDITION.getNumber())
          .setMessage(invalidActionVerboseMessage(actionDigest, preconditionFailure))
          .addDetails(Any.pack(preconditionFailure))
          .build();
      return null;
    }
  }

  private @Nullable Digest expectActionDigest(Operation operation) {
    ExecuteOperationMetadata metadata = expectExecuteOperationMetadata(operation);
    if (metadata == null) {
      return null;
    }
    return metadata.getActionDigest();
  }

  @Override
  public boolean putOperation(Operation operation) throws InterruptedException {
    String operationName = operation.getName();
    if (isQueued(operation)) {
      // destroy any monitors for this queued operation
      // any race should be resolved in a failure to requeue
      Watchdog requeuer = requeuers.remove(operationName);
      if (requeuer != null) {
        requeuer.stop();
      }
      Watchdog operationTimeoutDelay = operationTimeoutDelays.remove(operationName);
      if (operationTimeoutDelay != null) {
        operationTimeoutDelay.stop();
      }
    }
    if (!super.putOperation(operation)) {
      return false;
    }
    if (operation.getDone()) {
      // destroy requeue timer
      Watchdog requeuer = requeuers.remove(operationName);
      if (requeuer != null) {
        requeuer.stop();
      }
      // destroy action timed out failure
      Watchdog operationTimeoutDelay = operationTimeoutDelays.remove(operationName);
      if (operationTimeoutDelay != null) {
        operationTimeoutDelay.stop();
      }

      String operationStatus = "terminated";
      if (isCancelled(operation)) {
        operationStatus = "cancelled";
      } else if (isComplete(operation)) {
        operationStatus = "completed";
      }
      logger.log(Level.INFO, format("Operation %s was %s", operationName, operationStatus));
    } else if (isExecuting(operation)) {
      Watchdog requeuer = requeuers.get(operationName);
      if (requeuer == null) {
        // restore a requeuer if a worker indicates they are executing
        onDispatched(operation);
      } else {
        requeuer.pet();
      }

      // Create a delayed fuse timed out failure
      // This is in effect if the worker does not respond
      // within a configured delay with operation action timeout results
      com.google.rpc.Status.Builder status = com.google.rpc.Status.newBuilder();
      Action action = getActionForTimeoutMonitor(operation, status);
      if (action == null) {
        // prevent further activity of this operation, since it can not
        // transition to execution without independent provision of action blob
        // or reconfiguration of operation metadata
        // force an immediate error completion of the operation
        errorOperation(operation, RequestMetadata.getDefaultInstance(), status.build());
        return false;
      }
      Duration actionTimeout = null;
      if (action.hasTimeout()) {
        actionTimeout = action.getTimeout();
      } else if (config.hasDefaultActionTimeout()) {
        actionTimeout = config.getDefaultActionTimeout();
      }
      if (actionTimeout != null) {
        Duration delay = config.getOperationCompletedDelay();
        Duration timeout =
            Duration.newBuilder()
                .setSeconds(actionTimeout.getSeconds() + delay.getSeconds())
                .setNanos(actionTimeout.getNanos() + delay.getNanos())
                .build();
        // this is an overuse of Watchdog, we will never pet it
        Watchdog operationTimeoutDelay =
            new Watchdog(
                timeout,
                () -> {
                  operationTimeoutDelays.remove(operationName);
                  try {
                    expireOperation(operation);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }
                });
        operationTimeoutDelays.put(operationName, operationTimeoutDelay);
        new Thread(operationTimeoutDelay).start();
      }
    }
    return true;
  }

  private void onDispatched(Operation operation) {
    final String operationName = operation.getName();
    Duration timeout = config.getOperationPollTimeout();
    Watchdog requeuer =
        new Watchdog(
            timeout,
            () -> {
              logger.log(Level.INFO, format("REQUEUEING %s", operation.getName()));
              requeuers.remove(operationName);
              requeueOperation(operation);
            });
    requeuers.put(operation.getName(), requeuer);
    new Thread(requeuer).start();
  }

  Tree getCompleteTree(Digest rootDigest) {
    Tree.Builder tree = Tree.newBuilder();
    String pageToken = "";
    do {
      pageToken = getTree(rootDigest, 1024, pageToken, tree);
    } while (!pageToken.isEmpty());
    return tree.build();
  }

  @Override
  protected boolean matchOperation(Operation operation) throws InterruptedException {
    ExecuteOperationMetadata metadata = expectExecuteOperationMetadata(operation);
    Preconditions.checkState(metadata != null, "metadata not found");

    Action action =
        getUnchecked(
            expect(
                metadata.getActionDigest(),
                Action.parser(),
                newDirectExecutorService(),
                RequestMetadata.getDefaultInstance()));
    Preconditions.checkState(action != null, "action not found");

    Command command =
        getUnchecked(
            expect(
                action.getCommandDigest(),
                Command.parser(),
                newDirectExecutorService(),
                RequestMetadata.getDefaultInstance()));
    Preconditions.checkState(command != null, "command not found");

    Tree tree = getCompleteTree(action.getInputRootDigest());

    QueuedOperation queuedOperation =
        QueuedOperation.newBuilder().setAction(action).setCommand(command).setTree(tree).build();
    ByteString queuedOperationBlob = queuedOperation.toByteString();
    Digest queuedOperationDigest = getDigestUtil().compute(queuedOperationBlob);
    String operationName = operation.getName();
    try {
      putBlob(
          this,
          queuedOperationDigest,
          queuedOperationBlob,
          60,
          SECONDS,
          RequestMetadata.getDefaultInstance());
    } catch (StatusException | IOException e) {
      logger.log(Level.SEVERE, format("could not emplace queued operation: %s", operationName), e);
      return false;
    }

    ImmutableList.Builder<Worker> rejectedWorkers = new ImmutableList.Builder<>();
    boolean dispatched = false;
    WorkerQueue queue =
        queuedOperations.MatchEligibleQueue(createProvisions(command.getPlatform()));

    DequeueMatchSettings settings = new DequeueMatchSettings();
    synchronized (queue.workers) {
      while (!dispatched && !queue.workers.isEmpty()) {
        Worker worker = queue.workers.remove(0);
        if (!DequeueMatchEvaluator.shouldKeepOperation(settings, worker.getProvisions(), command)) {
          rejectedWorkers.add(worker);
        } else {
          QueueEntry queueEntry =
              QueueEntry.newBuilder()
                  // FIXME find a way to get this properly populated...
                  .setExecuteEntry(
                      ExecuteEntry.newBuilder()
                          .setOperationName(operationName)
                          .setActionDigest(metadata.getActionDigest())
                          .setStdoutStreamName(metadata.getStdoutStreamName())
                          .setStderrStreamName(metadata.getStderrStreamName())
                          .setQueuedTimestamp(Timestamps.fromMillis(System.currentTimeMillis())))
                  .setQueuedOperationDigest(queuedOperationDigest)
                  .setPlatform(command.getPlatform())
                  .build();
          dispatched = worker.getListener().onEntry(queueEntry);
          if (dispatched) {
            onDispatched(operation);
          }
        }
      }
      Iterables.addAll(queue.workers, rejectedWorkers.build());
    }
    return dispatched;
  }

  private SetMultimap<String, String> getOperationProvisions(Operation operation)
      throws InterruptedException {
    ExecuteOperationMetadata metadata = expectExecuteOperationMetadata(operation);
    Preconditions.checkState(metadata != null, "metadata not found");

    Action action =
        getUnchecked(
            expect(
                metadata.getActionDigest(),
                Action.parser(),
                newDirectExecutorService(),
                RequestMetadata.getDefaultInstance()));
    Preconditions.checkState(action != null, "action not found");

    Command command =
        getUnchecked(
            expect(
                action.getCommandDigest(),
                Command.parser(),
                newDirectExecutorService(),
                RequestMetadata.getDefaultInstance()));
    Preconditions.checkState(command != null, "command not found");

    return createProvisions(command.getPlatform());
  }

  @SuppressWarnings("ConstantConditions")
  private void matchSynchronized(Platform platform, MatchListener listener)
      throws InterruptedException {
    ImmutableList.Builder<Operation> rejectedOperations = ImmutableList.builder();
    boolean matched = false;
    SetMultimap<String, String> provisions = createProvisions(platform);
    WorkerQueue queue = queuedOperations.MatchEligibleQueue(provisions);
    while (!matched && !queue.operations.isEmpty()) {
      Operation operation = queue.operations.remove(0);
      ExecuteOperationMetadata metadata = expectExecuteOperationMetadata(operation);
      Preconditions.checkState(metadata != null, "metadata not found");

      Action action =
          getUnchecked(
              expect(
                  metadata.getActionDigest(),
                  Action.parser(),
                  newDirectExecutorService(),
                  RequestMetadata.getDefaultInstance()));
      Preconditions.checkState(action != null, "action not found");

      Command command =
          getUnchecked(
              expect(
                  action.getCommandDigest(),
                  Command.parser(),
                  newDirectExecutorService(),
                  RequestMetadata.getDefaultInstance()));
      Preconditions.checkState(command != null, "command not found");

      String operationName = operation.getName();

      DequeueMatchSettings settings = new DequeueMatchSettings();
      if (command == null) {
        cancelOperation(operationName);
      } else if (DequeueMatchEvaluator.shouldKeepOperation(settings, provisions, command)) {
        QueuedOperation queuedOperation =
            QueuedOperation.newBuilder()
                .setAction(action)
                .setCommand(command)
                .setTree(getCompleteTree(action.getInputRootDigest()))
                .build();
        ByteString queuedOperationBlob = queuedOperation.toByteString();
        Digest queuedOperationDigest = getDigestUtil().compute(queuedOperationBlob);
        // maybe do this elsewhere
        try {
          putBlob(
              this,
              queuedOperationDigest,
              queuedOperationBlob,
              60,
              SECONDS,
              RequestMetadata.getDefaultInstance());

          QueueEntry queueEntry =
              QueueEntry.newBuilder()
                  // FIXME find a way to get this properly populated...
                  .setExecuteEntry(
                      ExecuteEntry.newBuilder()
                          .setOperationName(operationName)
                          .setActionDigest(metadata.getActionDigest())
                          .setStdoutStreamName(metadata.getStdoutStreamName())
                          .setStderrStreamName(metadata.getStderrStreamName())
                          .setQueuedTimestamp(Timestamps.fromMillis(System.currentTimeMillis())))
                  .setQueuedOperationDigest(queuedOperationDigest)
                  .setPlatform(command.getPlatform())
                  .build();

          matched = true;
          if (listener.onEntry(queueEntry)) {
            onDispatched(operation);
          } else {
            enqueueOperation(operation);
          }
        } catch (StatusException | IOException e) {
          logger.log(
              Level.SEVERE, format("could not emplace queued operation: %s", operationName), e);
        }
      } else {
        rejectedOperations.add(operation);
      }
    }
    for (Operation operation : rejectedOperations.build()) {
      requeueOperation(operation);
    }
    if (!matched) {
      synchronized (queue.workers) {
        listener.setOnCancelHandler(() -> queuedOperations.removeWorker(listener));
        listener.onWaitStart();
        queuedOperations.AddWorker(provisions, listener);
      }
    }
  }

  @Override
  public void match(Platform platform, MatchListener listener) throws InterruptedException {
    WorkerQueue queue = queuedOperations.MatchEligibleQueue(createProvisions(platform));
    synchronized (queue.operations) {
      matchSynchronized(platform, listener);
    }
  }

  @Override
  public BackplaneStatus backplaneStatus() {
    BackplaneStatus.Builder status = BackplaneStatus.newBuilder();
    OperationQueueStatus.Builder queueStatus = status.getOperationQueueBuilder();
    long totalSize = 0;
    for (WorkerQueue queue : queuedOperations) {
      long size = queue.operations.size();
      queueStatus.addProvisionsBuilder().setName(queue.name).setSize(size);
      totalSize += size;
    }
    queueStatus.setSize(totalSize);
    // TODO dispatched - difficult to track
    // TODO active workers - available, but not with any discerning identifier, should rectify this
    return status.build();
  }

  @Override
  public ListenableFuture<Void> watchOperation(String operationName, Watcher watcher) {
    Operation operation = getOperation(operationName);
    try {
      watcher.observe(operation);
    } catch (Throwable t) {
      return immediateFailedFuture(t);
    }
    if (operation == null || operation.getDone()) {
      return immediateFuture(null);
    }
    WatchFuture watchFuture =
        new WatchFuture(watcher) {
          @Override
          protected void unwatch() {
            synchronized (watchers) {
              watchers.remove(operationName, this);
            }
          }
        };
    synchronized (watchers) {
      watchers.put(operationName, watchFuture);
    }
    operation = getOperation(operationName);
    if (!watchFuture.isDone() && operation == null || operation.getDone()) {
      // guarantee at least once delivery
      watchFuture.observe(operation);
    }
    return watchFuture;
  }

  @Override
  protected int getListOperationsDefaultPageSize() {
    return config.getListOperationsDefaultPageSize();
  }

  @Override
  protected int getListOperationsMaxPageSize() {
    return config.getListOperationsMaxPageSize();
  }

  @Override
  protected int getTreeDefaultPageSize() {
    return config.getTreeDefaultPageSize();
  }

  @Override
  protected int getTreeMaxPageSize() {
    return config.getTreeMaxPageSize();
  }

  @Override
  public String listOperations(
      int pageSize, String pageToken, String filter, ImmutableList.Builder<Operation> operations) {
    TokenizableIterator<Operation> iter = createOperationsIterator(pageToken);
    while (iter.hasNext() && pageSize != 0) {
      Operation operation = iter.next();
      operations.add(operation);
      if (pageSize > 0) {
        pageSize--;
      }
    }
    return iter.toNextPageToken();
  }

  @Override
  protected TokenizableIterator<DirectoryEntry> createTreeIterator(
      String reason, Digest rootDigest, String pageToken) {
    ExecutorService service = newDirectExecutorService();
    return new TreeIterator(
        (digest) -> {
          try {
            return expect(digest, Directory.parser(), service, RequestMetadata.getDefaultInstance())
                .get();
          } catch (ExecutionException e) {
            // should we have a special exception for our not found blob?
            Status status = Status.fromThrowable(e);
            if (status.getCode() != Code.NOT_FOUND) {
              logger.log(Level.SEVERE, "error fetching directory", e);
            }
            return null;
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
          }
        },
        rootDigest,
        pageToken);
  }

  @Override
  protected TokenizableIterator<Operation> createOperationsIterator(String pageToken) {
    Iterator<Operation> iter = outstandingOperations.iterator();
    final OperationIteratorToken token;
    if (!pageToken.isEmpty()) {
      try {
        token = OperationIteratorToken.parseFrom(BaseEncoding.base64().decode(pageToken));
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalArgumentException();
      }
      boolean paged = false;
      while (iter.hasNext() && !paged) {
        paged = iter.next().getName().equals(token.getOperationName());
      }
    } else {
      token = null;
    }
    return new TokenizableIterator<Operation>() {
      private OperationIteratorToken nextToken = token;

      @Override
      public boolean hasNext() {
        return iter.hasNext();
      }

      @Override
      public Operation next() {
        Operation operation = iter.next();
        nextToken =
            OperationIteratorToken.newBuilder().setOperationName(operation.getName()).build();
        return operation;
      }

      @Override
      public String toNextPageToken() {
        if (hasNext()) {
          return BaseEncoding.base64().encode(nextToken.toByteArray());
        }
        return "";
      }
    };
  }

  @Override
  protected Object operationLock() {
    return completedOperations;
  }

  @Override
  protected Logger getLogger() {
    return logger;
  }

  @Override
  public GetClientStartTimeResult getClientStartTime(GetClientStartTimeRequest request) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CasIndexResults reindexCas(@Nullable String hostName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public FindOperationsResults findOperations(String filterPredicate) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deregisterWorker(String workerName) {
    throw new UnsupportedOperationException();
  }
}
