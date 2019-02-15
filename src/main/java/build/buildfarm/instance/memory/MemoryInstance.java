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

import static build.buildfarm.instance.Utils.putBlob;
import static com.google.common.collect.Multimaps.synchronizedSetMultimap;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.lang.String.format;
import static java.util.Collections.synchronizedSortedMap;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.logging.Level.SEVERE;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.Platform;
import build.buildfarm.ac.ActionCache;
import build.buildfarm.ac.GrpcActionCache;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.cas.ContentAddressableStorages;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.TokenizableIterator;
import build.buildfarm.common.TreeIterator;
import build.buildfarm.common.TreeIterator.DirectoryEntry;
import build.buildfarm.common.function.InterruptingPredicate;
import build.buildfarm.common.Watchdog;
import build.buildfarm.common.Watcher;
import build.buildfarm.instance.AbstractServerInstance;
import build.buildfarm.instance.OperationsMap;
import build.buildfarm.instance.WatchFuture;
import build.buildfarm.v1test.ActionCacheConfig;
import build.buildfarm.v1test.ExecuteEntry;
import build.buildfarm.v1test.GrpcACConfig;
import build.buildfarm.v1test.MemoryInstanceConfig;
import build.buildfarm.v1test.OperationIteratorToken;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.QueuedOperation;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.PreconditionFailure;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;
import java.util.logging.Logger;

public class MemoryInstance extends AbstractServerInstance {
  private static final Logger logger = Logger.getLogger(MemoryInstance.class.getName());

  public static final String TIMEOUT_OUT_OF_BOUNDS =
      "A timeout specified is out of bounds with a configured range";

  private final MemoryInstanceConfig config;
  private final SetMultimap<String, WatchFuture> watchers;
  private final Map<String, ByteStringStreamSource> streams =
      new ConcurrentHashMap<String, ByteStringStreamSource>();
  private final List<Operation> queuedOperations = new ArrayList<Operation>();
  private final List<Worker> workers = new ArrayList<Worker>();
  private final Map<String, Watchdog> requeuers =
      new ConcurrentHashMap(new HashMap<String, Watchdog>());
  private final Map<String, Watchdog> operationTimeoutDelays =
      new ConcurrentHashMap(new HashMap<String, Watchdog>());
  private final OperationsMap outstandingOperations;
  private final Executor watcherExecutor;

  private static final class Worker {
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
        new OutstandingOperations());
  }

  @VisibleForTesting
  public MemoryInstance(
      String name,
      DigestUtil digestUtil,
      MemoryInstanceConfig config,
      ContentAddressableStorage contentAddressableStorage,
      SetMultimap<String, WatchFuture> watchers,
      Executor watcherExecutor,
      OperationsMap outstandingOperations) {
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

  private ByteStringStreamSource getSource(String name) {
    ByteStringStreamSource source = streams.get(name);
    if (source == null) {
      source = new ByteStringStreamSource();
      source.getOutputStream().getCommittedFuture().addListener(() -> streams.remove(name), directExecutor());
      streams.put(name, source);
    }
    return source;
  }

  @Override
  public CommittingOutputStream getStreamOutput(String name, long expectedSize) {
    return getSource(name).getOutputStream();
  }

  @Override
  public InputStream newStreamInput(String name, long offset) {
    return getSource(name).openStream();
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
  protected void onQueue(Operation operation, Action action) throws IOException, InterruptedException, StatusException {
    putBlob(this, digestUtil.compute(action), action.toByteString());
  }

  @Override
  protected void validateAction(
      Action action,
      PreconditionFailure.Builder preconditionFailure)
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

    super.validateAction(action, preconditionFailure);
  }

  @Override
  public boolean pollOperation(
      String operationName,
      ExecuteOperationMetadata.Stage stage) {
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

  @Override
  public boolean putOperation(Operation operation) throws InterruptedException {
    if (!super.putOperation(operation)) {
      return false;
    }
    String operationName = operation.getName();
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
      Action action = expectAction(operation);
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
    Duration timeout = config.getOperationPollTimeout();
    Watchdog requeuer = new Watchdog(timeout, () -> {
      logger.info("REQUEUEING " + operation.getName());
      requeueOperation(operation);
    });
    requeuers.put(operation.getName(), requeuer);
    new Thread(requeuer).start();
  }

  @Override
  protected boolean matchOperation(Operation operation) throws InterruptedException {
    ExecuteOperationMetadata metadata = expectExecuteOperationMetadata(operation);
    Preconditions.checkState(metadata != null, "metadata not found");

    Action action = getUnchecked(expect(metadata.getActionDigest(), Action.parser(), newDirectExecutorService()));
    Preconditions.checkState(action != null, "action not found");

    Command command = getUnchecked(expect(action.getCommandDigest(), Command.parser(), newDirectExecutorService()));
    Preconditions.checkState(command != null, "command not found");

    QueuedOperation queuedOperation = QueuedOperation.newBuilder()
        .setAction(action)
        .setCommand(command)
        // .addAllDirectories(directories)
        .build();
    ByteString queuedOperationBlob = queuedOperation.toByteString();
    Digest queuedOperationDigest = getDigestUtil().compute(queuedOperationBlob);
    String operationName = operation.getName();
    try {
      putBlob(this, queuedOperationDigest, queuedOperationBlob);
    } catch (StatusException|IOException e) {
      logger.log(SEVERE, format("could not emplace queued operation: %s", operationName), e);
      return false;
    }

    ImmutableList.Builder<Worker> rejectedWorkers = new ImmutableList.Builder<>();
    boolean dispatched = false;
    synchronized (workers) {
      while (!dispatched && !workers.isEmpty()) {
        Worker worker = workers.remove(0);
        if (!satisfiesRequirements(worker.getPlatform(), command)) {
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

      Action action = getUnchecked(expect(metadata.getActionDigest(), Action.parser(), newDirectExecutorService()));
      Preconditions.checkState(action != null, "action not found");

      Command command = getUnchecked(expect(action.getCommandDigest(), Command.parser(), newDirectExecutorService()));
      Preconditions.checkState(command != null, "command not found");

      String operationName = operation.getName();
      if (command == null) {
        cancelOperation(operationName);
      } else if (satisfiesRequirements(platform, command)) {
        QueuedOperation queuedOperation = QueuedOperation.newBuilder()
            .setAction(action)
            .setCommand(command)
            // .addAllDirectories(directories)
            .build();
        ByteString queuedOperationBlob = queuedOperation.toByteString();
        Digest queuedOperationDigest = getDigestUtil().compute(queuedOperationBlob);
        // maybe do this elsewhere
        try {
          putBlob(this, queuedOperationDigest, queuedOperationBlob);

          QueueEntry queueEntry = QueueEntry.newBuilder()
              // FIXME find a way to get this properly populated...
              .setExecuteEntry(ExecuteEntry.newBuilder()
                  .setOperationName(operationName)
                  .setActionDigest(metadata.getActionDigest())
                  .setStdoutStreamName(metadata.getStdoutStreamName())
                  .setStderrStreamName(metadata.getStderrStreamName()))
              .setQueuedOperationDigest(queuedOperationDigest)
              .build();

          matched = true;
          if (listener.onEntry(queueEntry)) {
            onDispatched(operation);
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

  private boolean satisfiesRequirements(Platform platform, Command command) throws InterruptedException {
    // string compare only
    // no duplicate names
    ImmutableMap.Builder<String, String> provisionsBuilder =
        new ImmutableMap.Builder<String, String>();
    for (Platform.Property property : platform.getPropertiesList()) {
      provisionsBuilder.put(property.getName(), property.getValue());
    }
    Map<String, String> provisions = provisionsBuilder.build();
    for (Platform.Property property : command.getPlatform().getPropertiesList()) {
      if (!provisions.containsKey(property.getName()) ||
          !provisions.get(property.getName()).equals(property.getValue())) {
        return false;
      }
    }
    return true;
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
      Digest rootDigest, String pageToken) {
    ExecutorService service = newDirectExecutorService();
    return new TreeIterator((digest) -> expect(digest, Directory.parser(), service), rootDigest, pageToken);
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
