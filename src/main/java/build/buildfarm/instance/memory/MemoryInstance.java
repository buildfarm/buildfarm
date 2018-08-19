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

import build.buildfarm.common.Watchdog;
import build.buildfarm.common.ContentAddressableStorage;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.instance.AbstractServerInstance;
import build.buildfarm.instance.TokenizableIterator;
import build.buildfarm.instance.TreeIterator;
import build.buildfarm.instance.TreeIterator.DirectoryEntry;
import build.buildfarm.v1test.MemoryInstanceConfig;
import build.buildfarm.v1test.OperationIteratorToken;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.devtools.remoteexecution.v1test.Action;
import com.google.devtools.remoteexecution.v1test.ActionResult;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.Directory;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata;
import com.google.devtools.remoteexecution.v1test.Platform;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.StatusException;
import java.io.IOException;
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
  private final Map<String, Operation> outstandingOperations;

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

  public MemoryInstance(String name, DigestUtil digestUtil, MemoryInstanceConfig config) {
    this(
        name,
        digestUtil,
        config,
        /*contentAddressableStorage=*/ new MemoryLRUContentAddressableStorage(config.getCasMaxSizeBytes()),
        /*watchers=*/ new ConcurrentHashMap<String, List<Predicate<Operation>>>(),
        /*outstandingOperations=*/ new TreeMap<String, Operation>());
  }

  @VisibleForTesting
  public MemoryInstance(
      String name,
      DigestUtil digestUtil,
      MemoryInstanceConfig config,
      ContentAddressableStorage contentAddressableStorage,
      Map<String, List<Predicate<Operation>>> watchers,
      Map<String, Operation> outstandingOperations) {
    super(
        name,
        digestUtil,
        contentAddressableStorage,
        /*actionCache=*/ new DelegateCASMap<ActionKey, ActionResult>(contentAddressableStorage, ActionResult.parser(), digestUtil),
        outstandingOperations,
        /*completedOperations=*/ new DelegateCASMap<String, Operation>(contentAddressableStorage, Operation.parser(), digestUtil),
        /*activeBlobWrites=*/ new ConcurrentHashMap<Digest, ByteString>());
    this.config = config;
    this.watchers = watchers;
    streams = new HashMap<String, ByteStringStreamSource>();
    queuedOperations = new ArrayList<Operation>();
    workers = new ArrayList<Worker>();
    requeuers = new HashMap<String, Watchdog>();
    operationTimeoutDelays = new HashMap<String, Watchdog>();
    this.outstandingOperations = outstandingOperations;
  }

  private ByteStringStreamSource getSource(String name) {
    ByteStringStreamSource source = streams.get(name);
    if (source == null) {
      source = new ByteStringStreamSource();
      source.getOutputStream().getCommittedFuture().addListener(() -> streams.remove(name), MoreExecutors.directExecutor());
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
  protected void updateOperationWatchers(Operation operation) {
    synchronized (watchers) {
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
  protected void onQueue(Operation operation, Action action) throws IOException, InterruptedException, StatusException {
    putBlob(this, digestUtil.compute(action), action.toByteString());
  }

  @Override
  protected void validateAction(Action action) throws InterruptedException {
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

      String operationStatus = "terminated";
      if (isCancelled(operation)) {
        operationStatus = "cancelled";
      } else if (isComplete(operation)) {
        operationStatus = "completed";
      }
      System.out.println(String.format("Operation %s was %s", operationName, operationStatus));
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
      System.out.println("REQUEUEING " + operation.getName());
      try {
        putOperation(operation);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    requeuers.put(operation.getName(), requeuer);
    new Thread(requeuer).start();
  }

  @Override
  protected boolean matchOperation(Operation operation) throws InterruptedException {
    ImmutableList.Builder<Worker> rejectedWorkers = new ImmutableList.Builder<>();
    boolean dispatched = false;
    synchronized (workers) {
      while (!dispatched && !workers.isEmpty()) {
        Worker worker = workers.remove(0);
        if (!satisfiesRequirements(worker.getPlatform(), operation)) {
          rejectedWorkers.add(worker);
        } else {
          // worker onMatch false return indicates inviability
          if (dispatched = worker.getListener().onOperation(operation)) {
            onDispatched(operation);
          }
        }
      }
      Iterables.addAll(workers, rejectedWorkers.build());
    }
    return dispatched;
  }

  private void matchSynchronized(Platform platform, MatchListener listener) throws InterruptedException {
    ImmutableList.Builder<Operation> rejectedOperations = new ImmutableList.Builder<Operation>();
    boolean matched = false;
    while (!matched && !queuedOperations.isEmpty()) {
      Operation operation = queuedOperations.remove(0);
      if (satisfiesRequirements(platform, operation)) {
        matched = true;
        if (listener.onOperation(operation)) {
          onDispatched(operation);
          /*
          for this context, we need to make the requeue go into a bucket during onOperation
        } else if (!requeueOnFailure) {
          rejectedOperations.add(operation);
          */
        }
      } else {
        rejectedOperations.add(operation);
      }
    }
    Iterables.addAll(queuedOperations, rejectedOperations.build());
    if (!matched) {
      synchronized (workers) {
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

  private boolean satisfiesRequirements(Platform platform, Operation operation) throws InterruptedException {
    Action action = expectAction(operation);
    // string compare only
    // no duplicate names
    ImmutableMap.Builder<String, String> provisionsBuilder =
      new ImmutableMap.Builder<String, String>();
    for (Platform.Property property : platform.getPropertiesList()) {
      provisionsBuilder.put(property.getName(), property.getValue());
    }
    Map<String, String> provisions = provisionsBuilder.build();
    for (Platform.Property property : action.getPlatform().getPropertiesList()) {
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
      boolean watchInitialState,
      Predicate<Operation> watcher) {
    if (watchInitialState) {
      Operation operation = getOperation(operationName);
      if (!watcher.test(operation)) {
        // watcher processed completed state
        return true;
      }
      if (operation == null || operation.getDone()) {
        // watcher did not process completed state
        return false;
      }
    }
    Operation completedOperation = null;
    synchronized (watchers) {
      List<Predicate<Operation>> operationWatchers = watchers.get(operationName);
      if (operationWatchers == null) {
        /* we can race on the synchronization and miss a done, where the
         * watchers list has been removed, making it necessary to check for the
         * operation within this context */
        Operation operation = getOperation(operationName);
        if (operation == null || !watchInitialState) {
          // missing operation with no initial state requires no handling
          // leave non-watchInitialState watchers of missing items to linger
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

  private List<Operation> sortedOperations() {
    return new ImmutableList.Builder<Operation>()
        .addAll(outstandingOperations.values())
        .build();
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
      Digest rootDigest, String pageToken) throws IOException, InterruptedException {
    return new TreeIterator(this::expectDirectory, rootDigest, pageToken);
  }

  @Override
  protected TokenizableIterator<Operation> createOperationsIterator(
      String pageToken) {
    Iterator<Operation> iter = sortedOperations().iterator();
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
}
