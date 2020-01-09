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

import static build.buildfarm.common.Actions.invalidActionMessage;
import static build.buildfarm.common.Actions.satisfiesRequirements;
import static build.buildfarm.common.Errors.VIOLATION_TYPE_INVALID;
import static build.buildfarm.common.Errors.VIOLATION_TYPE_MISSING;
import static build.buildfarm.instance.Utils.putBlob;
import static com.google.common.collect.Multimaps.synchronizedSetMultimap;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.catching;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.lang.String.format;
import static java.util.Collections.synchronizedSortedMap;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.WARNING;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecutionStage;
import build.bazel.remote.execution.v2.Platform;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.bazel.remote.execution.v2.Tree;
import build.buildfarm.ac.ActionCache;
import build.buildfarm.ac.GrpcActionCache;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.cas.ContentAddressableStorages;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.TokenizableIterator;
import build.buildfarm.common.TreeIterator;
import build.buildfarm.common.TreeIterator.DirectoryEntry;
import build.buildfarm.common.Watchdog;
import build.buildfarm.common.Watcher;
import build.buildfarm.common.Write;
import build.buildfarm.common.io.FeedbackOutputStream;
import build.buildfarm.instance.AbstractServerInstance;
import build.buildfarm.instance.OperationsMap;
import build.buildfarm.instance.WatchFuture;
import build.buildfarm.v1test.ActionCacheConfig;
import build.buildfarm.v1test.ExecuteEntry;
import build.buildfarm.v1test.GrpcACConfig;
import build.buildfarm.v1test.MemoryInstanceConfig;
import build.buildfarm.v1test.OperationIteratorToken;
import build.buildfarm.v1test.QueuedOperation;
import build.buildfarm.v1test.QueueEntry;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Durations;
import com.google.rpc.PreconditionFailure;
import io.grpc.Channel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusException;
import java.io.InputStream;
import java.io.IOException;
import java.util.ArrayList;
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
import java.util.logging.Logger;
import javax.annotation.Nullable;

public class MemoryInstance extends AbstractServerInstance {
  private static final Logger logger = Logger.getLogger(MemoryInstance.class.getName());

  public static final String TIMEOUT_OUT_OF_BOUNDS =
      "A timeout specified is out of bounds with a configured range";

  private final MemoryInstanceConfig config;
  private final SetMultimap<String, WatchFuture> watchers;
  private final LoadingCache<String, ByteStringStreamSource> streams = CacheBuilder.newBuilder()
      .expireAfterWrite(1, TimeUnit.HOURS)
      .removalListener(new RemovalListener<String, ByteStringStreamSource>() {
        @Override
        public void onRemoval(RemovalNotification<String, ByteStringStreamSource> notification) {
          try {
            notification.getValue().getOutput().close();
          } catch (IOException e) {
            logger.log(SEVERE, "error closing stream source " + notification.getKey(), e);
          }
        }
      })
      .build(new CacheLoader<String, ByteStringStreamSource>() {
        @Override
        public ByteStringStreamSource load(String name) {
          return newStreamSource(name);
        }
      });
  private final List<Operation> queuedOperations = Lists.newArrayList();
  private final List<Worker> workers;
  private final Map<String, Watchdog> requeuers;
  private final Map<String, Watchdog> operationTimeoutDelays;
  private final OperationsMap outstandingOperations;
  private final Executor watcherExecutor;

  static final class Worker {
    private final Platform platform;
    private final MatchListener listener;

    Worker(Platform platform, MatchListener listener) {
      this.platform = platform;
      this.listener = listener;
    }

    Platform getPlatform() {
      return platform;
    }

    MatchListener getListener() {
      return listener;
    }
  }

  static class OutstandingOperations implements OperationsMap {
    private final Map<String, Operation> map =
        synchronizedSortedMap(new TreeMap<>());

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

  public MemoryInstance(String name, DigestUtil digestUtil, MemoryInstanceConfig config) {
    this(
        name,
        digestUtil,
        config,
        ContentAddressableStorages.create(config.getCasConfig()),
        /* watchers=*/ synchronizedSetMultimap(
            MultimapBuilder
                .hashKeys()
                .hashSetValues(/* expectedValuesPerKey=*/ 1)
                .build()),
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
        MemoryInstance.createActionCache(config.getActionCacheConfig(), contentAddressableStorage, digestUtil),
        outstandingOperations,
        MemoryInstance.createCompletedOperationMap(contentAddressableStorage, digestUtil),
        /*activeBlobWrites=*/ new ConcurrentHashMap<Digest, ByteString>());
    this.config = config;
    this.watchers = watchers;
    this.outstandingOperations = outstandingOperations;
    this.watcherExecutor = watcherExecutor;
    this.workers = workers;
    this.requeuers = requeuers;
    this.operationTimeoutDelays = operationTimeoutDelays;
  }

  private static ActionCache createActionCache(ActionCacheConfig config, ContentAddressableStorage cas, DigestUtil digestUtil) {
    switch (config.getTypeCase()) {
      default:
      case TYPE_NOT_SET:
        throw new IllegalArgumentException("ActionCache config not set in config");
      case GRPC:
        return createGrpcActionCache(config.getGrpc());
      case DELEGATE_CAS:
        return createDelegateCASActionCache(cas, digestUtil);
    }
  }

  private static Channel createChannel(String target) {
    NettyChannelBuilder builder =
        NettyChannelBuilder.forTarget(target)
            .negotiationType(NegotiationType.PLAINTEXT);
    return builder.build();
  }

  private static ActionCache createGrpcActionCache(GrpcACConfig config) {
    Channel channel = createChannel(config.getTarget());
    return new GrpcActionCache(config.getInstanceName(), channel);
  }

  private static ActionCache createDelegateCASActionCache(ContentAddressableStorage cas, DigestUtil digestUtil) {
    return new ActionCache() {
      DelegateCASMap<ActionKey, ActionResult> map =
          new DelegateCASMap<>(cas, ActionResult.parser(), digestUtil);

      @Override
      public ActionResult get(ActionKey actionKey) {
        return map.get(actionKey);
      }

      @Override
      public void put(ActionKey actionKey, ActionResult actionResult) throws InterruptedException {
        map.put(actionKey, actionResult);
      }
    };
  }

  private static OperationsMap createCompletedOperationMap(ContentAddressableStorage cas, DigestUtil digestUtil) {
    return new OperationsMap() {
      DelegateCASMap<String, Operation> map =
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
    source.getClosedFuture().addListener(
        () -> streams.invalidate(name),
        directExecutor());
    return source;
  }

  ByteStringStreamSource getStreamSource(String name) {
    try {
      return streams.get(name);
    } catch (ExecutionException e) {
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
          long deadlineAfter,
          TimeUnit deadlineAfterUnits,
          Runnable onReadyHandler) {
        return getStreamSource(name).getOutput();
      }

      @Override
      public void reset() {
        streams.invalidate(name);
      }

      @Override
      public void addListener(Runnable onCompleted, Executor executor) {
        getStreamSource(name).getClosedFuture().addListener(onCompleted, executor);
      }
    };
  }

  @Override
  public InputStream newOperationStreamInput(
      String name,
      long offset,
      long deadlineAfter,
      TimeUnit deadlineAfterUnits,
      RequestMetadata requestMetadata) throws IOException {
    InputStream in = getStreamSource(name).openStream();
    in.skip(offset);
    return in;
  }

  @Override
  protected void enqueueOperation(Operation operation) {
    synchronized (queuedOperations) {
      Preconditions.checkState(!Iterables.any(queuedOperations, (queuedOperation) -> queuedOperation.getName().equals(operation.getName())));
      queuedOperations.add(operation);
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

    ExecuteOperationMetadata metadata = ExecuteOperationMetadata.newBuilder()
        .setActionDigest(actionKey.getDigest())
        .build();

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
      if (timeout.getSeconds() > maximum.getSeconds() ||
          (timeout.getSeconds() == maximum.getSeconds() && timeout.getNanos() > maximum.getNanos())) {
        preconditionFailure.addViolationsBuilder()
            .setType(VIOLATION_TYPE_INVALID)
            .setSubject(Durations.toString(timeout) + " > " + Durations.toString(maximum))
            .setDescription(TIMEOUT_OUT_OF_BOUNDS);
      }
    }

    super.validateAction(operationName, action, preconditionFailure, requestMetadata);
  }

  @Override
  public boolean pollOperation(
      String operationName,
      ExecutionStage.Value stage) {
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

  private Action getActionForTimeoutMonitor(Operation operation, com.google.rpc.Status.Builder status) throws InterruptedException {
    Digest actionDigest = expectActionDigest(operation);
    if (actionDigest == null) {
      logger.warning(format("Could not determine Action Digest for operation %s", operation.getName()));
      String message = String.format(
          "Could not determine Action Digest from Operation %s",
          operation.getName());
      status
          .setCode(com.google.rpc.Code.INTERNAL.getNumber())
          .setMessage(message);
      return null;
    }
    ByteString actionBlob = getBlob(actionDigest);
    if (actionBlob == null) {
      logger.warning(
          format(
              "Action %s for operation %s went missing, cannot initiate execution monitoring",
              DigestUtil.toString(actionDigest),
              operation.getName()));
      PreconditionFailure.Builder preconditionFailure = PreconditionFailure.newBuilder();
      preconditionFailure.addViolationsBuilder()
          .setType(VIOLATION_TYPE_MISSING)
          .setSubject("blobs/" + DigestUtil.toString(actionDigest))
          .setDescription(MISSING_ACTION);
      status
          .setCode(com.google.rpc.Code.FAILED_PRECONDITION.getNumber())
          .setMessage(invalidActionMessage(actionDigest))
          .addDetails(Any.pack(preconditionFailure.build()))
          .build();
      return null;
    }
    try {
      return Action.parseFrom(actionBlob);
    } catch (InvalidProtocolBufferException e) {
      logger.log(WARNING, format("Could not parse Action %s for Operation %s", DigestUtil.toString(actionDigest), operation.getName()), e);
      PreconditionFailure.Builder preconditionFailure = PreconditionFailure.newBuilder();
      preconditionFailure.addViolationsBuilder()
          .setType(VIOLATION_TYPE_INVALID)
          .setSubject(INVALID_ACTION)
          .setDescription("Action " + DigestUtil.toString(actionDigest));
      status
          .setCode(com.google.rpc.Code.FAILED_PRECONDITION.getNumber())
          .setMessage(invalidActionMessage(actionDigest))
          .addDetails(Any.pack(preconditionFailure.build()))
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
      Watchdog operationTimeoutDelay =
          operationTimeoutDelays.remove(operationName);
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
      Watchdog operationTimeoutDelay =
          operationTimeoutDelays.remove(operationName);
      if (operationTimeoutDelay != null) {
        operationTimeoutDelay.stop();
      }

      String operationStatus = "terminated";
      if (isCancelled(operation)) {
        operationStatus = "cancelled";
      } else if (isComplete(operation)) {
        operationStatus = "completed";
      }
      logger.info(format("Operation %s was %s", operationName, operationStatus));
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
        errorOperation(
            operation,
            RequestMetadata.getDefaultInstance(),
            status.build());
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
        Duration timeout = Duration.newBuilder()
            .setSeconds(actionTimeout.getSeconds() + delay.getSeconds())
            .setNanos(actionTimeout.getNanos() + delay.getNanos())
            .build();
        // this is an overuse of Watchdog, we will never pet it
        Watchdog operationTimeoutDelay = new Watchdog(timeout, () -> {
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
    Watchdog requeuer = new Watchdog(timeout, () -> {
      logger.info("REQUEUEING " + operationName);
      requeuers.remove(operationName);
      requeueOperation(operation);
    });
    requeuers.put(operationName, requeuer);
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

  // removes any instance of an existing listener in the workers list
  private void removeWorker(MatchListener listener) {
    synchronized (workers) {
      Iterator<Worker> iter = workers.iterator();
      while (iter.hasNext()) {
        if (iter.next().getListener() == listener) {
          iter.remove();
        }
      }
    }
  }

  @Override
  protected boolean matchOperation(Operation operation) throws InterruptedException {
    ExecuteOperationMetadata metadata = expectExecuteOperationMetadata(operation);
    Preconditions.checkState(metadata != null, "metadata not found");

    Action action = getUnchecked(expect(
        metadata.getActionDigest(),
        Action.parser(),
        newDirectExecutorService(),
        RequestMetadata.getDefaultInstance()));
    Preconditions.checkState(action != null, "action not found");

    Command command = getUnchecked(expect(
        action.getCommandDigest(),
        Command.parser(),
        newDirectExecutorService(),
        RequestMetadata.getDefaultInstance()));
    Preconditions.checkState(command != null, "command not found");

    Tree tree = getCompleteTree(action.getInputRootDigest());

    QueuedOperation queuedOperation = QueuedOperation.newBuilder()
        .setAction(action)
        .setCommand(command)
        .setTree(tree)
        .build();
    ByteString queuedOperationBlob = queuedOperation.toByteString();
    Digest queuedOperationDigest = getDigestUtil().compute(queuedOperationBlob);
    String operationName = operation.getName();
    try {
      putBlob(this, queuedOperationDigest, queuedOperationBlob, 60, SECONDS, RequestMetadata.getDefaultInstance());
    } catch (StatusException|IOException e) {
      logger.log(SEVERE, format("could not emplace queued operation: %s", operationName), e);
      return false;
    }

    ImmutableList.Builder<Worker> rejectedWorkers = new ImmutableList.Builder<>();
    boolean dispatched = false;
    synchronized (workers) {
      while (!dispatched && !workers.isEmpty()) {
        Worker worker = workers.remove(0);
        if (!satisfiesRequirements(worker.getPlatform(), command.getPlatform())) {
          rejectedWorkers.add(worker);
        } else {
          QueueEntry queueEntry = QueueEntry.newBuilder()
              // FIXME find a way to get this properly populated...
              .setExecuteEntry(ExecuteEntry.newBuilder()
                  .setOperationName(operationName)
                  .setActionDigest(metadata.getActionDigest())
                  .setStdoutStreamName(metadata.getStdoutStreamName())
                  .setStderrStreamName(metadata.getStderrStreamName()))
              .setQueuedOperationDigest(queuedOperationDigest)
              .setPlatform(command.getPlatform())
              .build();
          dispatched = worker.getListener().onEntry(queueEntry);
          if (dispatched) {
            onDispatched(operation);
          }
        }
      }
      Iterables.addAll(workers, rejectedWorkers.build());
    }
    return dispatched;
  }

  private void matchSynchronized(
      Platform platform,
      MatchListener listener) throws InterruptedException {
    ImmutableList.Builder<Operation> rejectedOperations = ImmutableList.builder();
    boolean matched = false;
    while (!matched && !queuedOperations.isEmpty()) {
      Operation operation = queuedOperations.remove(0);
      ExecuteOperationMetadata metadata = expectExecuteOperationMetadata(operation);
      Preconditions.checkState(metadata != null, "metadata not found");

      Action action = getUnchecked(expect(
          metadata.getActionDigest(),
          Action.parser(),
          newDirectExecutorService(),
          RequestMetadata.getDefaultInstance()));
      Preconditions.checkState(action != null, "action not found");

      Command command = getUnchecked(expect(
          action.getCommandDigest(),
          Command.parser(),
          newDirectExecutorService(),
          RequestMetadata.getDefaultInstance()));
      Preconditions.checkState(command != null, "command not found");

      String operationName = operation.getName();
      if (command == null) {
        cancelOperation(operationName);
      } else if (satisfiesRequirements(platform, command.getPlatform())) {
        QueuedOperation queuedOperation = QueuedOperation.newBuilder()
            .setAction(action)
            .setCommand(command)
            .setTree(getCompleteTree(action.getInputRootDigest()))
            .build();
        ByteString queuedOperationBlob = queuedOperation.toByteString();
        Digest queuedOperationDigest = getDigestUtil().compute(queuedOperationBlob);
        // maybe do this elsewhere
        try {
          putBlob(this, queuedOperationDigest, queuedOperationBlob, 60, SECONDS, RequestMetadata.getDefaultInstance());

          QueueEntry queueEntry = QueueEntry.newBuilder()
              // FIXME find a way to get this properly populated...
              .setExecuteEntry(ExecuteEntry.newBuilder()
                  .setOperationName(operationName)
                  .setActionDigest(metadata.getActionDigest())
                  .setStdoutStreamName(metadata.getStdoutStreamName())
                  .setStderrStreamName(metadata.getStderrStreamName()))
              .setQueuedOperationDigest(queuedOperationDigest)
              .setPlatform(command.getPlatform())
              .build();

          matched = true;
          if (listener.onEntry(queueEntry)) {
            onDispatched(operation);
          } else {
            enqueueOperation(operation);
          }
        } catch (StatusException|IOException e) {
          logger.log(SEVERE, format("could not emplace queued operation: %s", operationName), e);
        }
      } else {
        rejectedOperations.add(operation);
      }
    }
    for (Operation operation : rejectedOperations.build()) {
      requeueOperation(operation);
    }
    if (!matched) {
      synchronized(workers) {
        listener.setOnCancelHandler(() -> removeWorker(listener));
        listener.onWaitStart();
        workers.add(new Worker(platform, listener));
      }
    }
  }

  @Override
  public void match(Platform platform, MatchListener listener) throws InterruptedException {
    synchronized (queuedOperations) {
      matchSynchronized(platform, listener);
    }
  }

  @Override
  public ListenableFuture<Void> watchOperation(
      String operationName,
      Watcher watcher) {
    Operation operation = getOperation(operationName);
    try {
      watcher.observe(operation);
    } catch (Throwable t) {
      return immediateFailedFuture(t);
    }
    if (operation == null || operation.getDone()) {
      return immediateFuture(null);
    }
    WatchFuture watchFuture = new WatchFuture(watcher) {
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
  protected TokenizableIterator<DirectoryEntry> createTreeIterator(
      String reason, Digest rootDigest, String pageToken) {
    ExecutorService service = newDirectExecutorService();
    return new TreeIterator(
        (digest) -> catching(
          expect(digest, Directory.parser(), service, RequestMetadata.getDefaultInstance()),
          Exception.class,
          (e) -> {
            Status status = Status.fromThrowable(e);
            if (status.getCode() != Code.NOT_FOUND) {
              logger.log(SEVERE, "error fetching directory", e);
            }
            return null;
          },
          service),
        rootDigest,
        pageToken);
  }

  @Override
  protected TokenizableIterator<Operation> createOperationsIterator(
      String pageToken) {
    Iterator<Operation> iter = outstandingOperations.iterator();
    final OperationIteratorToken token;
    if (!pageToken.isEmpty()) {
      try {
        token = OperationIteratorToken.parseFrom(
            BaseEncoding.base64().decode(pageToken));
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
        nextToken = OperationIteratorToken.newBuilder()
            .setOperationName(operation.getName())
            .build();
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
  protected Object operationLock(String name) {
    /**
     * simple instance-wide locking on the completed operations
     */
    return completedOperations;
  }

  @Override
  protected Logger getLogger() {
    return logger;
  }
}
