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

import build.buildfarm.ac.ActionCache;
import build.buildfarm.ac.GrpcActionCache;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.cas.ContentAddressableStorages;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.instance.AbstractServerInstance;
import build.buildfarm.instance.OperationsMap;
import build.buildfarm.instance.TokenizableIterator;
import build.buildfarm.v1test.ActionCacheConfig;
import build.buildfarm.v1test.GrpcACConfig;
import build.buildfarm.v1test.MemoryInstanceConfig;
import build.buildfarm.v1test.OperationIteratorToken;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.io.BaseEncoding;
import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.Platform;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Channel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

public class MemoryInstance extends AbstractServerInstance {
  private final MemoryInstanceConfig config;
  private final Map<String, List<Predicate<Operation>>> watchers;
  private final Map<String, ByteStringStreamSource> streams;
  private final List<Operation> queuedOperations;
  private final List<Worker> workers;
  private final Map<String, Watchdog> requeuers;
  private final Map<String, Watchdog> operationTimeoutDelays;
  private final OperationsMap outstandingOperations;

  private static final class Worker {
    private final Platform platform;
    private final Predicate<Operation> onMatch;

    Worker(Platform platform, Predicate<Operation> onMatch) {
      this.platform = platform;
      this.onMatch = onMatch;
    }

    Platform getPlatform() {
      return platform;
    }

    boolean test(Operation operation) {
      return onMatch.test(operation);
    }
  }

  static class OutstandingOperations implements OperationsMap {
    private final Map<String, Operation> map = new TreeMap<>();

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
        /*watchers=*/ new ConcurrentHashMap<String, List<Predicate<Operation>>>(),
        new OutstandingOperations());
  }

  @VisibleForTesting
  public MemoryInstance(
      String name,
      DigestUtil digestUtil,
      MemoryInstanceConfig config,
      ContentAddressableStorage contentAddressableStorage,
      Map<String, List<Predicate<Operation>>> watchers,
      OperationsMap outstandingOperations) {
    super(
        name,
        digestUtil,
        contentAddressableStorage,
        MemoryInstance.createActionCache(config.getActionCacheConfig(), contentAddressableStorage, digestUtil),
        outstandingOperations,
        MemoryInstance.createCompletedOperationMap(contentAddressableStorage, digestUtil));
    this.config = config;
    this.watchers = watchers;
    streams = new HashMap<String, ByteStringStreamSource>();
    queuedOperations = new ArrayList<Operation>();
    workers = new ArrayList<Worker>();
    requeuers = new HashMap<String, Watchdog>();
    operationTimeoutDelays = new HashMap<String, Watchdog>();
    this.outstandingOperations = outstandingOperations;
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
      source = new ByteStringStreamSource(() -> streams.remove(name));
      streams.put(name, source);
    }
    return source;
  }

  @Override
  public OutputStream getStreamOutput(String name) {
    return getSource(name).getOutputStream();
  }

  @Override
  public InputStream newStreamInput(String name) {
    return getSource(name).openStream();
  }

  @Override
  protected void enqueueOperation(Operation operation) {
    synchronized(queuedOperations) {
      queuedOperations.add(operation);
    }
  }

  @Override
  protected void updateOperationWatchers(Operation operation) throws InterruptedException {
    synchronized(watchers) {
      List<Predicate<Operation>> operationWatchers =
          watchers.get(operation.getName());
      if (operationWatchers != null) {
        if (operation.getDone()) {
          watchers.remove(operation.getName());
        }
        long unfilteredWatcherCount = operationWatchers.size();
        super.updateOperationWatchers(operation);
        ImmutableList.Builder<Predicate<Operation>> filteredWatchers = new ImmutableList.Builder<>();
        long filteredWatcherCount = 0;
        for (Predicate<Operation> watcher : operationWatchers) {
          if (watcher.test(operation)) {
            filteredWatchers.add(watcher);
            filteredWatcherCount++;
          }
        }
        if (!operation.getDone() && filteredWatcherCount != unfilteredWatcherCount) {
          operationWatchers = new ArrayList<>();
          Iterables.addAll(operationWatchers, filteredWatchers.build());
          watchers.put(operation.getName(), operationWatchers);
        }
      } else {
        throw new IllegalStateException();
      }
    }
  }

  @Override
  protected Operation createOperation(ActionKey actionKey) {
    String name = createOperationName(UUID.randomUUID().toString());

    watchers.put(name, new ArrayList<Predicate<Operation>>());

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
  protected void onQueue(Operation operation, Action action) throws InterruptedException {
    putBlob(action.toByteString());
  }

  @Override
  protected void validateAction(Action action) {
    if (action.hasTimeout() && config.hasMaximumActionTimeout()) {
      Duration timeout = action.getTimeout();
      Duration maximum = config.getMaximumActionTimeout();
      Preconditions.checkState(
          timeout.getSeconds() < maximum.getSeconds() ||
          (timeout.getSeconds() == maximum.getSeconds() && timeout.getNanos() < maximum.getNanos()));
    }

    super.validateAction(action);
  }

  @Override
  public boolean pollOperation(
      String operationName,
      ExecuteOperationMetadata.Stage stage) {
    if (!super.pollOperation(operationName, stage)) {
      return false;
    }
    // pet the requeue watchdog
    requeuers.get(operationName).pet();
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
    } else if (isExecuting(operation)) {
      requeuers.get(operationName).pet();

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
        Watchdog operationTimeoutDelay = new Watchdog(timeout, () -> expireOperation(operation));
        operationTimeoutDelays.put(operationName, operationTimeoutDelay);
        new Thread(operationTimeoutDelay).start();
      }
    }
    return true;
  }

  private void onDispatched(Operation operation) {
    Duration timeout = config.getOperationPollTimeout();
    Watchdog requeuer = new Watchdog(timeout, () -> putOperation(operation));
    requeuers.put(operation.getName(), requeuer);
    new Thread(requeuer).start();
  }

  @Override
  protected boolean matchOperation(Operation operation) {
    Command command = expectCommand(operation);
    Preconditions.checkState(command != null, "command not found");

    ImmutableList.Builder<Worker> rejectedWorkers = new ImmutableList.Builder<>();
    boolean dispatched = false;
    synchronized(workers) {
      while (!dispatched && !workers.isEmpty()) {
        Worker worker = workers.remove(0);
        if (!satisfiesRequirements(worker.getPlatform(), command)) {
          rejectedWorkers.add(worker);
        } else {
          // worker onMatch false return indicates inviability
          if (dispatched = worker.test(operation)) {
            onDispatched(operation);
          }
        }
      }
      Iterables.addAll(workers, rejectedWorkers.build());
    }
    return dispatched;
  }

  @Override
  public void match(
      Platform platform,
      boolean requeueOnFailure,
      Predicate<Operation> onMatch)
      throws InterruptedException {
    synchronized(queuedOperations) {
      ImmutableList.Builder<Operation> rejectedOperations = new ImmutableList.Builder<Operation>();
      boolean matched = false;
      while (!matched && !queuedOperations.isEmpty()) {
        Operation operation = queuedOperations.remove(0);
        Command command = expectCommand(operation);
        if (command == null) {
          cancelOperation(operation.getName());
        } else {
          if (satisfiesRequirements(platform, command)) {
            matched = true;
            if (onMatch.test(operation)) {
              onDispatched(operation);
            } else if (!requeueOnFailure) {
              rejectedOperations.add(operation);
            }
          } else {
            rejectedOperations.add(operation);
          }
        }
      }
      Iterables.addAll(queuedOperations, rejectedOperations.build());
      if (!matched) {
        synchronized(workers) {
          workers.add(new Worker(platform, onMatch));
        }
      }
    }
  }

  private boolean satisfiesRequirements(Platform platform, Command command) {
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
  public boolean watchOperation(
      String operationName,
      Predicate<Operation> watcher) {
    Operation operation = getOperation(operationName);
    if (!watcher.test(operation)) {
      // watcher processed completed state
      return true;
    }
    if (operation == null || operation.getDone()) {
      // watcher did not process completed state
      return false;
    }
    Operation completedOperation = null;
    synchronized(watchers) {
      List<Predicate<Operation>> operationWatchers = watchers.get(operationName);
      if (operationWatchers == null) {
        /* we can race on the synchronization and miss a done, where the
         * watchers list has been removed, making it necessary to check for the
         * operation within this context */
        operation = getOperation(operationName);
        if (operation == null) {
          return true;
        }
        Preconditions.checkState(
            operation.getDone(),
            "watchers removed on incomplete operation");
        completedOperation = operation;
      } else {
        operationWatchers.add(watcher);
        return true;
      }
    }
    // the failed watcher test indicates that it did not handle the
    // completed state
    return !watcher.test(completedOperation);
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
  protected TokenizableIterator<Directory> createTreeIterator(
      Digest rootDigest, String pageToken) {
    return new TreeIterator(this, rootDigest, pageToken);
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
      } catch (InvalidProtocolBufferException ex) {
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
}
