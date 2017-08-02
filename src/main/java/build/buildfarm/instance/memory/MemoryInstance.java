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

import build.buildfarm.common.Digests;
import build.buildfarm.instance.TokenizableIterator;
import build.buildfarm.instance.AbstractServerInstance;
import build.buildfarm.v1test.MemoryInstanceConfig;
import build.buildfarm.v1test.OperationIteratorToken;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.io.BaseEncoding;
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
import java.util.function.Consumer;
import java.util.function.Function;

public class MemoryInstance extends AbstractServerInstance {
  private final MemoryInstanceConfig config;
  private final Map<String, List<Function<Operation, Boolean>>> watchers;
  private final Map<String, ByteStringStreamSource> streams;
  private final List<Operation> queuedOperations;
  private final List<Worker> workers;
  private final Map<String, Watchdog> requeuers;
  private final Map<String, Watchdog> operationTimeoutDelays;
  private final Map<String, Operation> outstandingOperations;
  private final Map<String, Operation> completedOperations;

  private static final class Worker {
    private final Platform platform;
    private final Function<Operation, Boolean> onMatch;

    Worker(Platform platform, Function<Operation, Boolean> onMatch) {
      this.platform = platform;
      this.onMatch = onMatch;
    }

    Platform getPlatform() {
      return platform;
    }

    boolean apply(Operation operation) {
      return onMatch.apply(operation);
    }
  }

  public MemoryInstance(String name, MemoryInstanceConfig config) {
    this(
        name,
        config,
        /*contentAddressableStorage=*/ new ConcurrentHashMap<Digest, ByteString>(),
        /*actionCache=*/ new ConcurrentHashMap<Digest, ActionResult>(),
        /*outstandingOperations=*/ new TreeMap<String, Operation>(),
        /*completedOperations=*/ new ConcurrentHashMap<String, Operation>());
  }

  private MemoryInstance(
      String name,
      MemoryInstanceConfig config,
      Map<Digest, ByteString> contentAddressableStorage,
      Map<Digest, ActionResult> actionCache,
      Map<String, Operation> outstandingOperations,
      Map<String, Operation> completedOperations) {
    super(
        name,
        contentAddressableStorage,
        actionCache,
        outstandingOperations,
        completedOperations);
    this.config = config;
    watchers = new ConcurrentHashMap<String, List<Function<Operation, Boolean>>>();
    streams = new HashMap<String, ByteStringStreamSource>();
    queuedOperations = new ArrayList<Operation>();
    workers = new ArrayList<Worker>();
    requeuers = new HashMap<String, Watchdog>();
    operationTimeoutDelays = new HashMap<String, Watchdog>();
    this.outstandingOperations = outstandingOperations;
    this.completedOperations = completedOperations;
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
  public Iterable<Digest> findMissingBlobs(Iterable<Digest> digests) {
    return Iterables.filter(
        digests, digest -> !contentAddressableStorage.containsKey(digest));
  }

  @Override
  public Iterable<Digest> putAllBlobs(Iterable<ByteString> blobs)
      throws IllegalArgumentException {
    if (Iterables.any(blobs, blob -> blob.size() == 0)) {
      throw new IllegalArgumentException();
    }

    ImmutableList.Builder<Digest> blobDigestsBuilder =
      new ImmutableList.Builder<Digest>();
    for (ByteString blob : blobs) {
      blobDigestsBuilder.add(putBlob(blob));
    }
    return blobDigestsBuilder.build();
  }

  @Override
  protected void enqueueOperation(Operation operation) {
    synchronized(queuedOperations) {
      queuedOperations.add(operation);
    }
  }

  @Override
  protected void updateOperationWatchers(Operation operation) {
    synchronized(watchers) {
      List<Function<Operation, Boolean>> operationWatchers =
          watchers.get(operation.getName());
      if (operationWatchers != null) {
        if (operation.getDone()) {
          watchers.remove(operation.getName());
        }
        long unfilteredWatcherCount = operationWatchers.size();
        super.updateOperationWatchers(operation);
        ImmutableList.Builder<Function<Operation, Boolean>> filteredWatchers = new ImmutableList.Builder<>();
        long filteredWatcherCount = 0;
        for (Function<Operation, Boolean> watcher : operationWatchers) {
          if (watcher.apply(operation)) {
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
  protected Operation createOperation(Action action) {
    String name = createOperationName(UUID.randomUUID().toString());

    watchers.put(name, new ArrayList<Function<Operation, Boolean>>());

    Digest actionDigest = Digests.computeDigest(action.toByteString());

    ExecuteOperationMetadata metadata = ExecuteOperationMetadata.newBuilder()
        .setActionDigest(actionDigest)
        .build();

    Operation.Builder operationBuilder = Operation.newBuilder()
        .setName(name)
        .setDone(false)
        .setMetadata(Any.pack(metadata));

    return operationBuilder.build();
  }

  @Override
  protected void onQueue(Operation operation, Action action) {
    putBlob(action.toByteString());
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
  public boolean putOperation(Operation operation) {
    if (!super.putOperation(operation)) {
      return false;
    }
    String operationName = operation.getName();
    if (isExecuting(operation)) {
      requeuers.get(operationName).pet();

      // Create a delayed fuse timed out failure
      // This is in effect if the worker does not respond
      // within a configured delay with operation action timeout results
      Action action = expectAction(operation);
      if (action.hasTimeout()) {
        Duration actionTimeout = action.getTimeout();
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
    } else if (isComplete(operation)) {
      // destroy requeue timer
      requeuers.remove(operationName).stop();
      // destroy action timed out failure
      Watchdog operationTimeoutDelay =
          operationTimeoutDelays.remove(operationName);
      if (operationTimeoutDelay != null) {
        operationTimeoutDelay.stop();
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
    ImmutableList.Builder<Worker> rejectedWorkers = new ImmutableList.Builder<>();
    boolean dispatched = false;
    synchronized(workers) {
      while (!dispatched && !workers.isEmpty()) {
        Worker worker = workers.remove(0);
        if (!satisfiesRequirements(worker.getPlatform(), operation)) {
          rejectedWorkers.add(worker);
        } else {
          // worker onMatch false return indicates inviability
          if (dispatched = worker.apply(operation)) {
            onDispatched(operation);
          }
        }
      }
      Iterables.addAll(workers, rejectedWorkers.build());
    }
    return dispatched;
  }

  @Override
  public void match(Platform platform, boolean requeueOnFailure, Function<Operation, Boolean> onMatch) {
    synchronized(queuedOperations) {
      ImmutableList.Builder<Operation> rejectedOperations = new ImmutableList.Builder<Operation>();
      boolean matched = false;
      while (!matched && !queuedOperations.isEmpty()) {
        Operation operation = queuedOperations.remove(0);
        if (satisfiesRequirements(platform, operation)) {
          matched = true;
          if (onMatch.apply(operation)) {
            onDispatched(operation);
          } else if (!requeueOnFailure) {
            rejectedOperations.add(operation);
          }
        } else {
          rejectedOperations.add(operation);
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

  private boolean satisfiesRequirements(Platform platform, Operation operation) {
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
      Function<Operation, Boolean> watcher) {
    if (watchInitialState) {
      Operation operation = getOperation(operationName);
      if (!watcher.apply(operation)) {
        return false;
      }
      if (operation.getDone()) {
        return true;
      }
    }
    synchronized(watchers) {
      List<Function<Operation, Boolean>> operationWatchers = watchers.get(operationName);
      if (operationWatchers == null) {
        return false;
      }
      operationWatchers.add(watcher);
    }
    return true;
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
  protected TokenizableIterator<Directory> createTreeIterator(
      Digest rootDigest, String pageToken) {
    return new TreeIterator(this, rootDigest, pageToken);
  }

  @Override
  protected TokenizableIterator<Operation> createOperationsIterator(
      String pageToken) {
    Iterator<Operation> iter = sortedOperations().iterator();
    OperationIteratorToken token;
    try {
      token = OperationIteratorToken.parseFrom(
          BaseEncoding.base64().decode(pageToken));
    } catch (InvalidProtocolBufferException ex) {
      throw new IllegalArgumentException();
    }
    if (!pageToken.isEmpty()) {
      boolean paged = true;
      while (iter.hasNext() && !paged) {
        paged = iter.next().getName().equals(pageToken);
      }
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
}
