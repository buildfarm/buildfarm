// Copyright 2017 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.instance.shard;

import static com.google.common.collect.Iterables.transform;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static redis.clients.jedis.params.ScanParams.SCAN_POINTER_START;

import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecutionStage;
import build.bazel.remote.execution.v2.Platform;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.bazel.remote.execution.v2.ToolDetails;
import build.buildfarm.backplane.Backplane;
import build.buildfarm.common.BuildfarmExecutors;
import build.buildfarm.common.CasIndexResults;
import build.buildfarm.common.CasIndexSettings;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.Visitor;
import build.buildfarm.common.Watcher;
import build.buildfarm.common.WorkerIndexer;
import build.buildfarm.common.config.BuildfarmConfigs;
import build.buildfarm.common.function.InterruptingRunnable;
import build.buildfarm.common.redis.BalancedRedisQueue.BalancedQueueEntry;
import build.buildfarm.common.redis.RedisClient;
import build.buildfarm.common.redis.Unified;
import build.buildfarm.instance.shard.ExecutionQueue.ExecutionQueueEntry;
import build.buildfarm.instance.shard.RedisShardSubscriber.TimedWatchFuture;
import build.buildfarm.v1test.BackplaneStatus;
import build.buildfarm.v1test.Digest;
import build.buildfarm.v1test.DispatchedOperation;
import build.buildfarm.v1test.ExecuteEntry;
import build.buildfarm.v1test.GetClientStartTime;
import build.buildfarm.v1test.GetClientStartTimeRequest;
import build.buildfarm.v1test.GetClientStartTimeResult;
import build.buildfarm.v1test.OperationChange;
import build.buildfarm.v1test.OperationQueueStatus;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.QueueStatus;
import build.buildfarm.v1test.QueuedOperationMetadata;
import build.buildfarm.v1test.ShardWorker;
import build.buildfarm.v1test.WorkerChange;
import build.buildfarm.v1test.WorkerType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.longrunning.Operation;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Timestamps;
import com.google.rpc.Code;
import com.google.rpc.PreconditionFailure;
import com.google.rpc.Status;
import io.grpc.Deadline;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.naming.ConfigurationException;
import lombok.extern.java.Log;
import redis.clients.jedis.AbstractPipeline;
import redis.clients.jedis.UnifiedJedis;

@Log
public class RedisShardBackplane implements Backplane {
  private static BuildfarmConfigs configs = BuildfarmConfigs.getInstance();

  private static final int workerSetMaxAge = 3; // seconds

  static final JsonFormat.Printer operationPrinter =
      JsonFormat.printer()
          .usingTypeRegistry(
              JsonFormat.TypeRegistry.newBuilder()
                  .add(ExecuteOperationMetadata.getDescriptor())
                  .add(QueuedOperationMetadata.getDescriptor())
                  .add(PreconditionFailure.getDescriptor())
                  .build());

  private final String source; // used in operation change publication
  private final boolean subscribeToBackplane;
  private final boolean runFailsafeOperation;
  private final Function<Operation, Operation> onPublish;
  private final Supplier<UnifiedJedis> jedisClusterFactory;

  private @Nullable InterruptingRunnable onUnsubscribe = null;
  private Thread subscriptionThread = null;
  private Thread failsafeOperationThread = null;
  private RedisShardSubscriber subscriber = null;
  private RedisShardSubscription operationSubscription = null;
  private ExecutorService subscriberService = null;
  private ExecutorService dequeueService = null;
  private ExecutorService pipelineExecutor = null;
  private @Nullable RedisClient client = null;

  private Deadline storageWorkersDeadline = null;
  private final Map<String, ShardWorker> storageWorkers = new ConcurrentHashMap<>();
  private final Supplier<Set<String>> recentExecuteWorkers;

  private DistributedState state = new DistributedState();

  public RedisShardBackplane(
      String source,
      boolean subscribeToBackplane,
      boolean runFailsafeOperation,
      Function<Operation, Operation> onPublish)
      throws ConfigurationException {
    this(
        source,
        subscribeToBackplane,
        runFailsafeOperation,
        onPublish,
        JedisClusterFactory.create(source));
  }

  public RedisShardBackplane(
      String source,
      boolean subscribeToBackplane,
      boolean runFailsafeOperation,
      Function<Operation, Operation> onPublish,
      Supplier<UnifiedJedis> jedisClusterFactory) {
    this.source = source;
    this.subscribeToBackplane = subscribeToBackplane;
    this.runFailsafeOperation = runFailsafeOperation;
    this.onPublish = onPublish;
    this.jedisClusterFactory = jedisClusterFactory;
    recentExecuteWorkers =
        Suppliers.memoizeWithExpiration(
            () -> {
              try {
                return client.call(this::fetchAndExpireExecuteWorkers).keySet();
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            },
            workerSetMaxAge,
            SECONDS);
  }

  @SuppressWarnings("NullableProblems")
  @Override
  public void setOnUnsubscribe(InterruptingRunnable onUnsubscribe) {
    this.onUnsubscribe = onUnsubscribe;
  }

  abstract static class ExecuteEntryListVisitor implements Visitor<BalancedQueueEntry> {
    protected abstract void visit(ExecuteEntry executeEntry, BalancedQueueEntry balancedQueueEntry);

    public void visit(BalancedQueueEntry balancedQueueEntry) {
      String entry = balancedQueueEntry.getValue();
      ExecuteEntry.Builder executeEntry = ExecuteEntry.newBuilder();
      try {
        JsonFormat.parser().merge(entry, executeEntry);
        visit(executeEntry.build(), balancedQueueEntry);
      } catch (InvalidProtocolBufferException e) {
        log.log(Level.FINER, "invalid ExecuteEntry json: " + entry, e);
      }
    }
  }

  private Instant convertToMilliInstant(String value, String key) {
    if (value != null) {
      try {
        return Instant.ofEpochMilli(Long.parseLong(value));
      } catch (NumberFormatException e) {
        log.log(Level.SEVERE, format("invalid expiration %s for %s", value, key));
      }
    }
    return null;
  }

  private void scanProcessing(UnifiedJedis jedis, Consumer<String> onOperationName, Instant now) {
    state.prequeue.visitDequeue(
        jedis,
        new ExecuteEntryListVisitor() {
          @Override
          protected void visit(ExecuteEntry executeEntry, BalancedQueueEntry balancedQueueEntry) {
            String executionName = executeEntry.getOperationName();
            String value = state.processingExecutions.get(jedis, executionName);
            long processingTimeout_ms = configs.getBackplane().getProcessingTimeoutMillis();

            // get the operation's expiration
            Instant expiresAt = convertToMilliInstant(value, executionName);

            // if expiration is invalid, add a valid one.
            if (expiresAt == null) {
              expiresAt = now.plusMillis(processingTimeout_ms);
              String keyValue = String.format("%d", expiresAt.toEpochMilli());
              // persist the flag for at least an hour, and at most 10 times longer than the timeout
              // the key identifies so that we don't loop with the flag expired, resetting the
              // unaccounted for operation
              long expire_s = Math.max(3600, MILLISECONDS.toSeconds(processingTimeout_ms) * 10);
              state.processingExecutions.insert(jedis, executionName, keyValue, expire_s);
            }

            // handle expiration
            if (now.isBefore(expiresAt)) {
              onOperationName.accept(executionName);
            } else {
              if (state.prequeue.removeFromDequeue(jedis, balancedQueueEntry)) {
                state.processingExecutions.remove(jedis, executionName);
              }
            }
          }
        });
  }

  private void scanDispatching(UnifiedJedis jedis, Consumer<String> onOperationName, Instant now) {
    state.executionQueue.visitDequeue(
        jedis,
        new Visitor<>() {
          @Override
          public void visit(ExecutionQueueEntry executionQueueEntry) {
            QueueEntry queueEntry = executionQueueEntry.getQueueEntry();
            String executionName = queueEntry.getExecuteEntry().getOperationName();
            String value = state.dispatchingExecutions.get(jedis, executionName);
            long dispatchingTimeout_ms = configs.getBackplane().getDispatchingTimeoutMillis();

            // get the operation's expiration
            Instant expiresAt = convertToMilliInstant(value, executionName);

            // if expiration is invalid, add a valid one.
            if (expiresAt == null) {
              expiresAt = now.plusMillis(dispatchingTimeout_ms);
              String keyValue = String.format("%d", expiresAt.toEpochMilli());
              // persist the flag for at least an hour, and at most 10 times longer than the timeout
              // the key identifies so that we don't loop with the flag expired, resetting the
              // unaccounted for operation
              long expire_s = Math.max(3600, MILLISECONDS.toSeconds(dispatchingTimeout_ms) * 10);
              state.dispatchingExecutions.insert(jedis, executionName, keyValue, expire_s);
            }

            // handle expiration
            if (now.isBefore(expiresAt)) {
              onOperationName.accept(executionName);
            } else {
              if (state.executionQueue.removeFromDequeue(jedis, executionQueueEntry)) {
                state.dispatchingExecutions.remove(jedis, executionName);
              }
            }
          }
        });
  }

  private void scanPrequeue(UnifiedJedis jedis, Consumer<String> onOperationName) {
    state.prequeue.visit(
        jedis,
        new ExecuteEntryListVisitor() {
          @Override
          protected void visit(ExecuteEntry executeEntry, BalancedQueueEntry executeEntryJson) {
            onOperationName.accept(executeEntry.getOperationName());
          }
        });
  }

  private void scanQueue(UnifiedJedis jedis, Consumer<String> onOperationName) {
    state.executionQueue.visit(
        jedis,
        new Visitor<>() {
          @Override
          public void visit(ExecutionQueueEntry executionQueueEntry) {
            QueueEntry queueEntry = executionQueueEntry.getQueueEntry();
            onOperationName.accept(queueEntry.getExecuteEntry().getOperationName());
          }
        });
  }

  private void scanDispatched(UnifiedJedis jedis, Consumer<String> onOperationName) {
    for (String executionName : state.dispatchedExecutions.keys(jedis)) {
      onOperationName.accept(executionName);
    }
  }

  private void updateWatchers(UnifiedJedis jedis) {
    Instant now = Instant.now();
    Instant expiresAt = nextExpiresAt(now);
    Set<String> expiringChannels = Sets.newHashSet(subscriber.expiredWatchedOperationChannels(now));
    Consumer<String> resetChannel =
        executionName -> {
          String channel = executionChannel(executionName);
          if (expiringChannels.remove(channel)) {
            subscriber.resetWatchers(channel, expiresAt);
          }
        };

    if (!expiringChannels.isEmpty()) {
      log.log(
          Level.FINER,
          format("Scan %d watches, %s, expiresAt: %s", expiringChannels.size(), now, expiresAt));

      log.log(Level.FINER, "Scan prequeue");
      // scan prequeue, pet watches
      scanPrequeue(jedis, resetChannel);
    }

    // scan processing, create ttl key if missing, remove dead entries, pet live watches
    scanProcessing(jedis, resetChannel, now);

    if (!expiringChannels.isEmpty()) {
      log.log(Level.FINER, "Scan queue");
      // scan queue, pet watches
      scanQueue(jedis, resetChannel);
    }

    // scan dispatching, create ttl key if missing, remove dead entries, pet live watches
    scanDispatching(jedis, resetChannel, now);

    if (!expiringChannels.isEmpty()) {
      log.log(Level.FINER, "Scan dispatched");
      // scan dispatched pet watches
      scanDispatched(jedis, resetChannel);
    }

    //
    // filter watches on expiration
    // delete the operation?
    // update expired watches with null operation
    for (String channel : expiringChannels) {
      Operation operation = getExecution(jedis, parseExecutionChannel(channel));
      if (operation == null || !operation.getDone()) {
        publishExpiration(jedis, channel, now);
      } else {
        subscriber.onOperation(channel, onPublish.apply(operation), expiresAt);
      }
    }
  }

  static String printOperationChange(OperationChange operationChange)
      throws InvalidProtocolBufferException {
    return operationPrinter.print(operationChange);
  }

  void publish(
      Consumer<String> onMessage, Instant effectiveAt, OperationChange.Builder operationChange) {
    try {
      String operationChangeJson =
          printOperationChange(
              operationChange.setEffectiveAt(toTimestamp(effectiveAt)).setSource(source).build());
      onMessage.accept(operationChangeJson);
    } catch (InvalidProtocolBufferException e) {
      log.log(Level.SEVERE, "error printing operation change", e);
      // very unlikely, printer would have to fail
    }
  }

  void publishReset(Consumer<String> onMessage, Operation operation) {
    Instant effectiveAt = Instant.now();
    Instant expiresAt = nextExpiresAt(effectiveAt);
    publish(
        onMessage,
        Instant.now(),
        OperationChange.newBuilder()
            .setReset(
                OperationChange.Reset.newBuilder()
                    .setExpiresAt(toTimestamp(expiresAt))
                    .setOperation(operation)
                    .build()));
  }

  void publishReset(AbstractPipeline pipeline, Operation operation) {
    String channel = executionChannel(operation.getName());
    publishReset(message -> pipeline.publish(channel, message), operation);
  }

  void publishReset(UnifiedJedis jedis, Operation operation) {
    String channel = executionChannel(operation.getName());
    publishReset(message -> jedis.publish(channel, message), operation);
  }

  static Timestamp toTimestamp(Instant instant) {
    return Timestamp.newBuilder()
        .setSeconds(instant.getEpochSecond())
        .setNanos(instant.getNano())
        .build();
  }

  void publishExpiration(UnifiedJedis jedis, String channel, Instant effectiveAt) {
    publish(
        message -> jedis.publish(channel, message),
        effectiveAt,
        OperationChange.newBuilder()
            .setExpire(OperationChange.Expire.newBuilder().setForce(false).build()));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public void updateWatchedIfDone(UnifiedJedis jedis) {
    List<String> operationChannels = subscriber.watchedOperationChannels();
    if (operationChannels.isEmpty()) {
      return;
    }

    Instant now = Instant.now();
    List<String> operationChannelNames =
        operationChannels.stream()
            .map(RedisShardBackplane::parseExecutionChannel)
            .collect(Collectors.toList());

    for (Operation operation : state.executions.get(jedis, operationChannelNames)) {
      if (operation == null || operation.getDone()) {
        if (operation != null) {
          operation = onPublish.apply(operation);
        }
        subscriber.onOperation(
            executionChannel(operation.getName()), operation, nextExpiresAt(now));
        log.log(
            Level.FINER,
            format(
                "operation %s done due to %s",
                operation.getName(), operation == null ? "null" : "completed"));
      }
    }
  }

  private Instant nextExpiresAt(Instant from) {
    return from.plusSeconds(10);
  }

  private void startSubscriptionThread() {
    ListMultimap<String, TimedWatchFuture> watchers =
        Multimaps.synchronizedListMultimap(
            MultimapBuilder.linkedHashKeys().arrayListValues().build());
    subscriberService = BuildfarmExecutors.getSubscriberPool();
    subscriber =
        new RedisShardSubscriber(
            watchers,
            storageWorkers,
            WorkerType.STORAGE.getNumber(),
            configs.getBackplane().getWorkerChannel(),
            subscriberService);

    operationSubscription =
        new RedisShardSubscription(
            subscriber,
            /* onUnsubscribe= */ () -> {
              subscriptionThread = null;
              if (onUnsubscribe != null) {
                onUnsubscribe.runInterruptibly();
              }
            },
            /* onReset= */ this::updateWatchedIfDone,
            /* subscriptions= */ subscriber::subscribedChannels,
            client);

    // use Executors...
    subscriptionThread = new Thread(operationSubscription, "Operation Subscription");

    subscriptionThread.start();
  }

  @SuppressWarnings("ConstantConditions")
  private void startFailsafeOperationThread() {
    failsafeOperationThread =
        new Thread(
            () -> {
              while (!Thread.currentThread().isInterrupted()) {
                try {
                  SECONDS.sleep(10);
                  client.run(this::updateWatchers);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  break;
                } catch (Exception e) {
                  log.log(Level.SEVERE, "error while updating watchers in failsafe", e);
                }
              }
            },
            "Failsafe Operation");

    failsafeOperationThread.start();
  }

  @Override
  public void start(String clientPublicName) throws IOException {
    // Construct a single redis client to be used throughout the entire backplane.
    // We wish to avoid various synchronous and error handling issues that could occur when using
    // multiple clients.
    start(new RedisClient(jedisClusterFactory.get()), clientPublicName);
  }

  private void start(RedisClient client, String clientPublicName) throws IOException {
    // Create containers that make up the backplane
    start(client, client.call(DistributedStateCreator::create), clientPublicName);
  }

  @VisibleForTesting
  void start(RedisClient client, DistributedState state, String clientPublicName)
      throws IOException {
    this.client = client;
    this.state = state;
    if (subscribeToBackplane) {
      startSubscriptionThread();
    }
    dequeueService = BuildfarmExecutors.getDequeuePool();
    if (runFailsafeOperation) {
      startFailsafeOperationThread();
    }
    pipelineExecutor = BuildfarmExecutors.getPipelinePool();

    // Record client start time
    client.call(
        jedis -> jedis.set("startTime/" + clientPublicName, Long.toString(new Date().getTime())));
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Override
  public synchronized void stop() throws InterruptedException {
    if (failsafeOperationThread != null) {
      failsafeOperationThread.interrupt();
      failsafeOperationThread.join();
      log.log(Level.FINER, "failsafeOperationThread has been stopped");
    }
    if (operationSubscription != null) {
      operationSubscription.stop();
      if (subscriptionThread != null) {
        subscriptionThread.join();
      }
      log.log(Level.FINER, "subscriptionThread has been stopped");
    }
    if (pipelineExecutor != null) {
      pipelineExecutor.shutdown();
      if (pipelineExecutor.awaitTermination(10, SECONDS)) {
        log.log(Level.FINER, "pipelineExecutor has been stopped");
      } else {
        log.log(Level.WARNING, "pipelineExecutor has not stopped");
      }
    }
    if (dequeueService != null) {
      dequeueService.shutdown();
      if (dequeueService.awaitTermination(10, SECONDS)) {
        log.log(Level.FINER, "dequeueService has been stopped");
      } else {
        log.log(Level.WARNING, "dequeueService has not stopped");
      }
    }
    if (subscriberService != null) {
      subscriberService.shutdown();
      if (subscriberService.awaitTermination(10, SECONDS)) {
        log.log(Level.FINER, "subscriberService has been stopped");
      } else {
        log.log(Level.WARNING, "subscriberService has not stopped");
      }
    }
    if (client != null) {
      client.close();
      client = null;
      log.log(Level.FINER, "client has been closed");
    }
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public boolean isStopped() {
    return client.isClosed();
  }

  @Override
  public ListenableFuture<Void> watchExecution(String executionName, Watcher watcher) {
    TimedWatcher timedWatcher =
        new TimedWatcher(nextExpiresAt(Instant.now())) {
          @Override
          public void observe(Operation operation) {
            watcher.observe(operation);
          }
        };
    return subscriber.watch(executionChannel(executionName), timedWatcher);
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public void addWorker(ShardWorker shardWorker) throws IOException {
    String json = JsonFormat.printer().print(shardWorker);
    Timestamp effectiveAt = Timestamps.fromMillis(shardWorker.getFirstRegisteredAt());
    WorkerChange.Add add =
        WorkerChange.Add.newBuilder()
            .setEffectiveAt(effectiveAt)
            .setWorkerType(shardWorker.getWorkerType())
            .build();
    String workerChangeJson =
        JsonFormat.printer()
            .print(
                WorkerChange.newBuilder()
                    .setEffectiveAt(toTimestamp(Instant.now()))
                    .setName(shardWorker.getEndpoint())
                    .setAdd(add)
                    .build());
    client.call(
        jedis -> {
          // could rework with an hget to publish prior, but this seems adequate, and
          // we are the only guaranteed source
          if (addWorkerByType(jedis, shardWorker, json)) {
            jedis.publish(configs.getBackplane().getWorkerChannel(), workerChangeJson);
            return true;
          }
          return false;
        });
  }

  private boolean addWorkerByType(UnifiedJedis jedis, ShardWorker shardWorker, String json) {
    int type = shardWorker.getWorkerType();
    if (type == 0) {
      return false; // no destination
    }
    boolean result = true;
    if ((type & WorkerType.EXECUTE.getNumber()) == WorkerType.EXECUTE.getNumber()) {
      result = state.executeWorkers.insert(jedis, shardWorker.getEndpoint(), json) && result;
    }
    if ((type & WorkerType.STORAGE.getNumber()) == WorkerType.STORAGE.getNumber()) {
      result = state.storageWorkers.insert(jedis, shardWorker.getEndpoint(), json) && result;
    }
    return result;
  }

  private boolean removeWorkerAndPublish(
      UnifiedJedis jedis, String name, String changeJson, boolean storage) {
    boolean removedAny = state.executeWorkers.remove(jedis, name);
    if (storage && state.storageWorkers.remove(jedis, name)) {
      jedis.publish(configs.getBackplane().getWorkerChannel(), changeJson);
      return true;
    }
    return removedAny;
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public boolean removeWorker(String name, String reason) throws IOException {
    WorkerChange workerChange =
        WorkerChange.newBuilder()
            .setName(name)
            .setRemove(WorkerChange.Remove.newBuilder().setSource(source).setReason(reason).build())
            .build();
    String workerChangeJson = JsonFormat.printer().print(workerChange);
    return storageWorkers.remove(name) != null
        && client.call(
            jedis -> removeWorkerAndPublish(jedis, name, workerChangeJson, /* storage= */ true));
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public CasIndexResults reindexCas() throws IOException {
    CasIndexSettings settings = new CasIndexSettings();
    settings.casQuery = configs.getBackplane().getCasPrefix() + ":*";
    settings.scanAmount = 10000;
    return client.call(jedis -> WorkerIndexer.removeWorkerIndexesFromCas(jedis, settings));
  }

  private static String tokenFromRedisCursor(String cursor) {
    return cursor.equals(SCAN_POINTER_START) ? SENTINEL_PAGE_TOKEN : cursor;
  }

  @Override
  public ScanResult<Operation> scanExecutions(String cursor, int count) throws IOException {
    redis.clients.jedis.resps.ScanResult<Operation> scanResult =
        client.call(jedis -> state.executions.scan(jedis, cursor, count));
    return new ScanResult<>(tokenFromRedisCursor(scanResult.getCursor()), scanResult.getResult());
  }

  @Override
  public ScanResult<String> scanToolInvocations(String cursor, int count) throws IOException {
    redis.clients.jedis.resps.ScanResult<String> scanResult =
        client.call(jedis -> state.toolInvocations.scan(jedis, cursor, count));
    return new ScanResult<>(tokenFromRedisCursor(scanResult.getCursor()), scanResult.getResult());
  }

  @Override
  public ScanResult<String> scanCorrelatedInvocations(String cursor, int count) throws IOException {
    redis.clients.jedis.resps.ScanResult<String> scanResult =
        client.call(jedis -> state.correlatedInvocations.scan(jedis, cursor, count));
    return new ScanResult<>(tokenFromRedisCursor(scanResult.getCursor()), scanResult.getResult());
  }

  @Override
  public ScanResult<String> scanCorrelatedInvocationIndexEntries(
      String cursor, int count, String keyMatch) throws IOException {
    redis.clients.jedis.resps.ScanResult<String> scanResult =
        client.call(
            jedis -> state.correlatedInvocationsIndex.scan(jedis, cursor, count, keyMatch + "=*"));
    return new ScanResult<>(tokenFromRedisCursor(scanResult.getCursor()), scanResult.getResult());
  }

  @Override
  public ScanResult<String> scanCorrelatedInvocationIndexKeys(String cursor, int count)
      throws IOException {
    redis.clients.jedis.resps.ScanResult<String> scanResult =
        client.call(jedis -> state.correlatedInvocationsIndex.scan(jedis, cursor, count));
    return new ScanResult<>(
        tokenFromRedisCursor(scanResult.getCursor()),
        Lists.newArrayList(
            Sets.newLinkedHashSet(
                transform(scanResult.getResult(), entry -> entry.split("=")[0]))));
  }

  @Override
  public void deregisterWorker(String workerName) throws IOException {
    removeWorker(workerName, "Requested shutdown");
  }

  /**
   * Returns a new set containing copies of the storage workers. Note: This method does not grant
   * access to the shared storage set.
   */
  @Override
  public Set<String> getStorageWorkers() throws IOException {
    refreshStorageWorkersIfExpired();
    return new HashSet<>(storageWorkers.keySet());
  }

  @Override
  public Map<String, Long> getWorkersStartTimeInEpochSecs(Set<String> workerNames)
      throws IOException {
    refreshStorageWorkersIfExpired();
    Map<String, Long> workerAndStartTime = new HashMap<>();
    workerNames.forEach(
        worker -> {
          ShardWorker workerInfo = storageWorkers.get(worker);
          if (workerInfo != null) {
            workerAndStartTime.put(
                worker, MILLISECONDS.toSeconds(workerInfo.getFirstRegisteredAt()));
          }
        });
    return workerAndStartTime;
  }

  private synchronized void refreshStorageWorkersIfExpired() throws IOException {
    if (storageWorkersDeadline == null || storageWorkersDeadline.isExpired()) {
      synchronized (storageWorkers) {
        Map<String, ShardWorker> newWorkers = client.call(this::fetchAndExpireStorageWorkers);
        storageWorkers.clear();
        storageWorkers.putAll(newWorkers);
      }
      storageWorkersDeadline = Deadline.after(workerSetMaxAge, SECONDS);
    }
  }

  private CasWorkerMap createCasWorkerMap(UnifiedJedis jedis) {
    return new JedisCasWorkerMap(
        jedis, configs.getBackplane().getCasPrefix(), configs.getBackplane().getCasExpire());
  }

  @Override
  public long getDigestInsertTime(Digest blobDigest) throws IOException {
    return client.call(jedis -> createCasWorkerMap(jedis).insertTime(blobDigest));
  }

  private synchronized Set<String> getExecuteWorkers() throws IOException {
    try {
      return recentExecuteWorkers.get();
    } catch (RuntimeException e) {
      // unwrap checked exception mask
      log.log(Level.WARNING, "getExecuteWorkers failed", e);
      Throwable cause = e.getCause();
      if (cause != null) {
        Throwables.throwIfInstanceOf(cause, IOException.class);
      }
      throw e;
    }
  }

  // When performing a graceful scale down of workers, the backplane can provide worker names to the
  // scale-down service. The algorithm in which the backplane chooses these workers can be made more
  // sophisticated in the future. But for now, we'll give back n random workers.
  public List<String> suggestedWorkersToScaleDown(int numWorkers) throws IOException {
    // get all workers
    List<String> allWorkers = new ArrayList<>(getStorageWorkers());

    // ensure selection amount is in range [0 - size]
    numWorkers = Math.max(0, Math.min(numWorkers, allWorkers.size()));

    // select n workers
    return randomN(allWorkers, numWorkers);
  }

  public static <T> List<T> randomN(List<T> list, int n) {
    return Stream.generate(
            () -> list.remove((int) (list.size() * ThreadLocalRandom.current().nextDouble())))
        .limit(Math.min(list.size(), n))
        .collect(Collectors.toList());
  }

  private void removeInvalidWorkers(
      UnifiedJedis jedis, long testedAt, List<ShardWorker> workers, boolean storage) {
    if (!workers.isEmpty()) {
      for (ShardWorker worker : workers) {
        String name = worker.getEndpoint();
        String reason =
            format("registration expired at %d, tested at %d", worker.getExpireAt(), testedAt);
        WorkerChange workerChange =
            WorkerChange.newBuilder()
                .setEffectiveAt(toTimestamp(Instant.now()))
                .setName(name)
                .setRemove(
                    WorkerChange.Remove.newBuilder().setSource(source).setReason(reason).build())
                .build();
        try {
          String workerChangeJson = JsonFormat.printer().print(workerChange);
          removeWorkerAndPublish(jedis, name, workerChangeJson, storage);
        } catch (InvalidProtocolBufferException e) {
          log.log(Level.SEVERE, "error printing workerChange", e);
        }
      }
    }
  }

  private Map<String, ShardWorker> fetchAndExpireStorageWorkers(UnifiedJedis jedis) {
    return fetchAndExpireWorkers(jedis, state.storageWorkers.asMap(jedis), /* storage= */ true);
  }

  private Map<String, ShardWorker> fetchAndExpireExecuteWorkers(UnifiedJedis jedis) {
    return fetchAndExpireWorkers(jedis, state.executeWorkers.asMap(jedis), /* storage= */ false);
  }

  private Map<String, ShardWorker> fetchAndExpireWorkers(
      UnifiedJedis jedis, Map<String, String> workers, boolean publish) {
    long now = System.currentTimeMillis();
    Map<String, ShardWorker> returnWorkers = Maps.newConcurrentMap();
    ImmutableList.Builder<ShardWorker> invalidWorkers = ImmutableList.builder();
    for (Map.Entry<String, String> entry : workers.entrySet()) {
      String json = entry.getValue();
      String name = entry.getKey();
      try {
        if (json == null) {
          invalidWorkers.add(ShardWorker.newBuilder().setEndpoint(name).build());
        } else {
          ShardWorker.Builder builder = ShardWorker.newBuilder();
          JsonFormat.parser().merge(json, builder);
          ShardWorker worker = builder.build();
          if (worker.getExpireAt() <= now) {
            invalidWorkers.add(worker);
          } else {
            returnWorkers.put(worker.getEndpoint(), worker);
          }
        }
      } catch (InvalidProtocolBufferException e) {
        invalidWorkers.add(ShardWorker.newBuilder().setEndpoint(name).build());
      }
    }
    removeInvalidWorkers(jedis, now, invalidWorkers.build(), publish);
    return returnWorkers;
  }

  private static ActionResult parseActionResult(String json) {
    try {
      ActionResult.Builder builder = ActionResult.newBuilder();
      JsonFormat.parser().merge(json, builder);
      return builder.build();
    } catch (InvalidProtocolBufferException e) {
      return null;
    }
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public ActionResult getActionResult(ActionKey actionKey) throws IOException {
    String json = client.call(jedis -> state.actionCache.get(jedis, actionKey.toString()));
    if (json == null) {
      return null;
    }

    ActionResult actionResult = parseActionResult(json);
    if (actionResult == null) {
      client.run(jedis -> removeActionResult(jedis, actionKey));
    }
    return actionResult;
  }

  // we do this by action hash only, so that we can use RequestMetadata to filter
  @SuppressWarnings("ConstantConditions")
  @Override
  public void blacklistAction(String actionId) throws IOException {
    client.run(
        jedis ->
            state.blockedActions.insert(
                jedis, actionId, "", configs.getBackplane().getActionBlacklistExpire()));
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public void putActionResult(ActionKey actionKey, ActionResult actionResult) throws IOException {
    String json = JsonFormat.printer().print(actionResult);
    client.run(
        jedis ->
            state.actionCache.insert(
                jedis, actionKey.toString(), json, configs.getBackplane().getActionCacheExpire()));
  }

  private void removeActionResult(UnifiedJedis jedis, ActionKey actionKey) {
    state.actionCache.remove(jedis, actionKey.toString());
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public void removeActionResult(ActionKey actionKey) throws IOException {
    client.run(jedis -> removeActionResult(jedis, actionKey));
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public void removeActionResults(Iterable<ActionKey> actionKeys) throws IOException {
    // convert action keys to strings
    List<String> keyNames = new ArrayList<>();
    actionKeys.forEach(key -> keyNames.add(key.toString()));

    client.run(jedis -> state.actionCache.remove(jedis, keyNames));
  }

  @Override
  public void adjustBlobLocations(
      Digest blobDigest, Set<String> addWorkers, Set<String> removeWorkers) throws IOException {
    client.run(jedis -> createCasWorkerMap(jedis).adjust(blobDigest, addWorkers, removeWorkers));
  }

  @Override
  public void addBlobLocation(Digest blobDigest, String workerName) throws IOException {
    client.run(jedis -> createCasWorkerMap(jedis).add(blobDigest, workerName));
  }

  @Override
  public void addBlobsLocation(Iterable<Digest> blobDigests, String workerName) throws IOException {
    client.run(jedis -> createCasWorkerMap(jedis).addAll(blobDigests, workerName));
  }

  @Override
  public void removeBlobLocation(Digest blobDigest, String workerName) throws IOException {
    client.run(jedis -> createCasWorkerMap(jedis).remove(blobDigest, workerName));
  }

  @Override
  public void removeBlobsLocation(Iterable<Digest> blobDigests, String workerName)
      throws IOException {
    client.run(jedis -> createCasWorkerMap(jedis).removeAll(blobDigests, workerName));
  }

  @Override
  public String getBlobLocation(Digest blobDigest) throws IOException {
    return client.call(jedis -> createCasWorkerMap(jedis).getAny(blobDigest));
  }

  @Override
  public Set<String> getBlobLocationSet(Digest blobDigest) throws IOException {
    return client.call(jedis -> createCasWorkerMap(jedis).get(blobDigest));
  }

  @Override
  public Map<Digest, Set<String>> getBlobDigestsWorkers(Iterable<Digest> blobDigests)
      throws IOException {
    return client.call(jedis -> createCasWorkerMap(jedis).getMap(blobDigests));
  }

  public static WorkerChange parseWorkerChange(String workerChangeJson)
      throws InvalidProtocolBufferException {
    WorkerChange.Builder workerChange = WorkerChange.newBuilder();
    JsonFormat.parser().merge(workerChangeJson, workerChange);
    return workerChange.build();
  }

  public static OperationChange parseOperationChange(String operationChangeJson)
      throws InvalidProtocolBufferException {
    OperationChange.Builder operationChange = OperationChange.newBuilder();
    Executions.getParser().merge(operationChangeJson, operationChange);
    return operationChange.build();
  }

  private Operation getExecution(UnifiedJedis jedis, String executionName) {
    return state.executions.get(jedis, executionName);
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public Operation getExecution(String executionName) throws IOException {
    return client.call(jedis -> getExecution(jedis, executionName));
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public boolean putOperation(Operation operation, ExecutionStage.Value stage) throws IOException {
    boolean queue = stage == ExecutionStage.Value.QUEUED;
    boolean complete = !queue && operation.getDone();
    boolean publish = !queue && stage != ExecutionStage.Value.UNKNOWN;

    String json;
    try {
      json = operationPrinter.print(operation);
    } catch (InvalidProtocolBufferException e) {
      log.log(Level.SEVERE, "error printing operation " + operation.getName(), e);
      return false;
    }

    Operation publishOperation;
    if (publish) {
      publishOperation = onPublish.apply(operation);
    } else {
      publishOperation = null;
    }

    String name = operation.getName();
    client.run(
        jedis -> {
          state.executions.insert(jedis, name, json);
          if (publishOperation != null) {
            publishReset(jedis, publishOperation);
          }
          if (complete) {
            completeOperation(jedis, name);
          }
        });
    return true;
  }

  private void queue(
      UnifiedJedis jedis,
      String executionName,
      List<Platform.Property> provisions,
      String queueEntryJson,
      int priority) {
    if (state.dispatchedExecutions.remove(jedis, executionName)) {
      log.log(Level.WARNING, format("removed dispatched execution %s", executionName));
    }
    state.executionQueue.push(jedis, provisions, queueEntryJson, priority);
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public void queue(QueueEntry queueEntry, Operation operation) throws IOException {
    String executionName = operation.getName();
    String operationJson = operationPrinter.print(operation);
    String queueEntryJson = JsonFormat.printer().print(queueEntry);
    Operation publishOperation = onPublish.apply(operation);
    int priority = queueEntry.getExecuteEntry().getExecutionPolicy().getPriority();
    client.run(
        jedis -> {
          state.executions.insert(jedis, executionName, operationJson);
          queue(
              jedis,
              operation.getName(),
              queueEntry.getPlatform().getPropertiesList(),
              queueEntryJson,
              priority);
          publishReset(jedis, publishOperation);
        });
  }

  @Override
  public ScanResult<Operation> scanExecutions(String toolInvocationId, String cursor, int count)
      throws IOException {
    redis.clients.jedis.resps.ScanResult<Operation> scanResult =
        client.call(
            jedis ->
                state.executions.findByToolInvocationId(jedis, toolInvocationId, cursor, count));
    return new ScanResult<>(tokenFromRedisCursor(scanResult.getCursor()), scanResult.getResult());
  }

  @Override
  public ScanResult<String> scanToolInvocations(
      String correlatedInvocationsId, String cursor, int count) throws IOException {
    redis.clients.jedis.resps.ScanResult<String> scanResult =
        client.call(
            jedis ->
                state.correlatedInvocations.scan(jedis, correlatedInvocationsId, cursor, count));
    return new ScanResult<>(tokenFromRedisCursor(scanResult.getCursor()), scanResult.getResult());
  }

  @Override
  public ScanResult<String> scanCorrelatedInvocations(
      String scope, String value, String cursor, int count) throws IOException {
    redis.clients.jedis.resps.ScanResult<String> scanResult =
        client.call(
            jedis ->
                state.correlatedInvocationsIndex.scan(jedis, scope + "=" + value, cursor, count));
    return new ScanResult<>(tokenFromRedisCursor(scanResult.getCursor()), scanResult.getResult());
  }

  @Override
  public ScanResult<DispatchedOperation> scanDispatchedOperations(String cursor, int count)
      throws IOException {
    ImmutableList.Builder<DispatchedOperation> builder = new ImmutableList.Builder<>();
    redis.clients.jedis.resps.ScanResult<Map.Entry<String, String>> scanResult =
        client.call(jedis -> state.dispatchedExecutions.scan(jedis, cursor, count));
    // executor work queue?
    for (Map.Entry<String, String> entry : scanResult.getResult()) {
      try {
        DispatchedOperation.Builder dispatchedOperationBuilder = DispatchedOperation.newBuilder();
        JsonFormat.parser().merge(entry.getValue(), dispatchedOperationBuilder);
        builder.add(dispatchedOperationBuilder.build());
      } catch (InvalidProtocolBufferException e) {
        log.log(Level.SEVERE, format("invalid dispatched operation %s", entry.getKey()), e);
      }
    }
    return new ScanResult(tokenFromRedisCursor(scanResult.getCursor()), builder.build());
  }

  private ExecuteEntry deprequeueOperation(UnifiedJedis jedis) throws InterruptedException {
    BalancedQueueEntry balancedQueueEntry = state.prequeue.take(jedis, dequeueService);
    if (balancedQueueEntry == null) {
      return null;
    }

    ExecuteEntry.Builder executeEntryBuilder = ExecuteEntry.newBuilder();
    try {
      JsonFormat.parser().merge(balancedQueueEntry.getValue(), executeEntryBuilder);
      ExecuteEntry executeEntry = executeEntryBuilder.build();
      String executionName = executeEntry.getOperationName();

      Operation operation = keepaliveExecution(executionName);
      // publish so that watchers reset their timeout
      publishReset(jedis, operation);

      // destroy the processing entry and ttl
      if (!state.prequeue.removeFromDequeue(jedis, balancedQueueEntry)) {
        log.log(
            Level.SEVERE,
            format("could not remove %s from %s", executionName, state.prequeue.getDequeueName()));
        return null;
      }
      state.processingExecutions.remove(jedis, executionName);
      return executeEntry;
    } catch (InvalidProtocolBufferException e) {
      log.log(Level.SEVERE, "error parsing execute entry", e);
      return null;
    }
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public ExecuteEntry deprequeueOperation() throws IOException, InterruptedException {
    return client.blockingCall(this::deprequeueOperation);
  }

  private @Nullable QueueEntry dispatchOperation(
      UnifiedJedis jedis, List<Platform.Property> provisions) throws InterruptedException {
    ExecutionQueueEntry executionQueueEntry =
        state.executionQueue.dequeue(jedis, provisions, dequeueService);
    if (executionQueueEntry == null) {
      return null;
    }

    QueueEntry queueEntry = executionQueueEntry.getQueueEntry();
    String executionName = queueEntry.getExecuteEntry().getOperationName();
    Operation operation = keepaliveExecution(executionName);
    Unified unified = (Unified) jedis;
    try (AbstractPipeline pipeline = unified.pipelined(pipelineExecutor)) {
      publishReset(pipeline, operation);

      long requeueAt =
          System.currentTimeMillis() + configs.getBackplane().getDispatchingTimeoutMillis();
      DispatchedOperation o =
          DispatchedOperation.newBuilder()
              .setQueueEntry(queueEntry)
              .setRequeueAt(requeueAt)
              .build();
      try {
        String dispatchedOperationJson = JsonFormat.printer().print(o);

        state.dispatchedExecutions.insertIfMissing(
            pipeline, executionName, dispatchedOperationJson);
      } catch (InvalidProtocolBufferException e) {
        log.log(Level.SEVERE, "error printing dispatched operation", e);
        // very unlikely, printer would have to fail
      }

      state.executionQueue.removeFromDequeue(pipeline, executionQueueEntry);
      state.dispatchingExecutions.remove(pipeline, executionName);
    }

    // Return an entry so that if it needs re-queued, it will have the correct "requeue attempts".
    return queueEntry.toBuilder().setRequeueAttempts(queueEntry.getRequeueAttempts() + 1).build();
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public QueueEntry dispatchOperation(List<Platform.Property> provisions)
      throws IOException, InterruptedException {
    return client.blockingCall(jedis -> dispatchOperation(jedis, provisions));
  }

  String printPollOperation(QueueEntry queueEntry, long requeueAt)
      throws InvalidProtocolBufferException {
    DispatchedOperation o =
        DispatchedOperation.newBuilder().setQueueEntry(queueEntry).setRequeueAt(requeueAt).build();
    return JsonFormat.printer().print(o);
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public void rejectOperation(QueueEntry queueEntry) throws IOException {
    String executionName = queueEntry.getExecuteEntry().getOperationName();
    String queueEntryJson = JsonFormat.printer().print(queueEntry);
    String dispatchedEntryJson = printPollOperation(queueEntry, 0);
    client.run(
        jedis -> {
          if (isBlacklisted(jedis, queueEntry.getExecuteEntry().getRequestMetadata())) {
            pollExecution(
                jedis, executionName, dispatchedEntryJson); // complete our lease to error operation
          } else {
            Operation operation = getExecution(jedis, executionName);
            boolean requeue =
                operation != null && !operation.getDone(); // operation removed or completed somehow
            if (state.dispatchedExecutions.remove(jedis, executionName) && requeue) {
              int priority = queueEntry.getExecuteEntry().getExecutionPolicy().getPriority();
              state.executionQueue.push(
                  jedis, queueEntry.getPlatform().getPropertiesList(), queueEntryJson, priority);
            }
          }
        });
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public boolean pollExecution(QueueEntry queueEntry, ExecutionStage.Value stage, long requeueAt)
      throws IOException {
    String executionName = queueEntry.getExecuteEntry().getOperationName();
    String json;
    try {
      json = printPollOperation(queueEntry, requeueAt);
    } catch (InvalidProtocolBufferException e) {
      log.log(Level.SEVERE, "error printing dispatched execution " + executionName, e);
      return false;
    }
    return client.call(jedis -> pollExecution(jedis, executionName, json));
  }

  boolean pollExecution(UnifiedJedis jedis, String executionName, String dispatchedOperationJson) {
    if (state.dispatchedExecutions.exists(jedis, executionName)) {
      if (!state.dispatchedExecutions.insert(jedis, executionName, dispatchedOperationJson)) {
        return true;
      }
      /* someone else beat us to the punch, delete our incorrectly added key */
      state.dispatchedExecutions.remove(jedis, executionName);
    }
    return false;
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public Operation mergeExecution(ActionKey actionKey) throws IOException {
    return client.call(jedis -> state.executions.merge(jedis, actionKey.toString()));
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public void unmergeExecution(ActionKey actionKey) throws IOException {
    client.run(jedis -> state.executions.unmerge(jedis, actionKey.toString()));
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public boolean prequeue(ExecuteEntry executeEntry, Operation execution, boolean ignoreMerge)
      throws IOException {
    String toolInvocationId = executeEntry.getRequestMetadata().getToolInvocationId();
    String executionName = execution.getName();
    String operationJson = operationPrinter.print(execution);
    String executeEntryJson = JsonFormat.printer().print(executeEntry);
    Operation publishExecution = onPublish.apply(execution);
    int priority = executeEntry.getExecutionPolicy().getPriority();
    ActionKey actionKey = DigestUtil.asActionKey(executeEntry.getActionDigest());
    return client.call(
        jedis -> {
          if (state.executions.create(jedis, actionKey.toString(), executionName, operationJson)
              || ignoreMerge) {
            if (!toolInvocationId.isEmpty()) {
              state.toolInvocations.add(jedis, toolInvocationId, executionName);
            }
            state.prequeue.offer(jedis, executeEntryJson, priority);
            publishReset(jedis, publishExecution);
            return true;
          }
          // execution should be merged, indicates as much
          state.executions.remove(jedis, executionName);
          return false;
        });
  }

  private Operation keepaliveExecution(String executionName) {
    return Operation.newBuilder().setName(executionName).build();
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public void queueing(String executionName) throws IOException {
    Operation operation = keepaliveExecution(executionName);
    // publish so that watchers reset their timeout
    client.run(jedis -> publishReset(jedis, operation));
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public void requeueDispatchedExecution(QueueEntry queueEntry) throws IOException {
    String queueEntryJson = JsonFormat.printer().print(queueEntry);
    String executionName = queueEntry.getExecuteEntry().getOperationName();
    Operation publishOperation = keepaliveExecution(executionName);
    int priority = queueEntry.getExecuteEntry().getExecutionPolicy().getPriority();
    client.run(
        jedis -> {
          queue(
              jedis,
              executionName,
              queueEntry.getPlatform().getPropertiesList(),
              queueEntryJson,
              priority);
          publishReset(jedis, publishOperation);
        });
  }

  private void completeOperation(UnifiedJedis jedis, String executionName) {
    state.dispatchedExecutions.remove(jedis, executionName);
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public void completeOperation(String executionName) throws IOException {
    client.run(jedis -> completeOperation(jedis, executionName));
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public void deleteOperation(String executionName) throws IOException {
    Operation o =
        Operation.newBuilder()
            .setName(executionName)
            .setDone(true)
            .setError(Status.newBuilder().setCode(Code.UNAVAILABLE.getNumber()).build())
            .build();

    client.run(
        jedis -> {
          completeOperation(jedis, executionName);
          // FIXME find a way to get rid of this thing from the queue by name
          // jedis.lrem(config.getQueuedOperationsListName(), 0, executionName);
          state.executions.remove(jedis, executionName);

          publishReset(jedis, o);
        });
  }

  String executionChannel(String executionName) {
    return configs.getBackplane().getOperationChannelPrefix() + ":" + executionName;
  }

  public static String parseExecutionChannel(String channel) {
    // should probably verify prefix
    return channel.split(":")[1];
  }

  @Override
  public Boolean propertiesEligibleForQueue(List<Platform.Property> provisions) {
    return state.executionQueue.isEligible(provisions);
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public boolean isBlacklisted(RequestMetadata requestMetadata) throws IOException {
    if (requestMetadata.getToolInvocationId().isEmpty()
        && requestMetadata.getActionId().isEmpty()) {
      return false;
    }
    return client.call(jedis -> isBlacklisted(jedis, requestMetadata));
  }

  private boolean isBlacklisted(UnifiedJedis jedis, RequestMetadata requestMetadata) {
    boolean isActionBlocked =
        (!requestMetadata.getActionId().isEmpty()
            && state.blockedActions.exists(jedis, requestMetadata.getActionId()));
    boolean isInvocationBlocked =
        (!requestMetadata.getToolInvocationId().isEmpty()
            && state.blockedInvocations.exists(jedis, requestMetadata.getToolInvocationId()));
    return isActionBlocked || isInvocationBlocked;
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public boolean canQueue() throws IOException {
    return client.call(jedis -> state.executionQueue.canQueue(jedis));
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public boolean canPrequeue() throws IOException {
    return client.call(jedis -> state.prequeue.canQueue(jedis));
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public BackplaneStatus backplaneStatus() throws IOException {
    return client.call(this::backplaneStatus);
  }

  private BackplaneStatus backplaneStatus(UnifiedJedis jedis) throws IOException {
    Unified unified = (Unified) jedis;
    Set<String> executeWorkers = getExecuteWorkers();
    Set<String> storageWorkers = getStorageWorkers();
    try (AbstractPipeline pipeline = unified.pipelined(pipelineExecutor)) {
      Supplier<QueueStatus> prequeue = state.prequeue.status(pipeline);
      Supplier<OperationQueueStatus> operationQueue = state.executionQueue.status(pipeline);
      Supplier<Long> dispatchedSize = state.dispatchedExecutions.size(pipeline);
      pipeline.sync();
      return BackplaneStatus.newBuilder()
          .addAllActiveExecuteWorkers(executeWorkers)
          .addAllActiveStorageWorkers(storageWorkers)
          .addAllActiveWorkers(Sets.union(executeWorkers, storageWorkers))
          .setPrequeue(prequeue.get())
          .setOperationQueue(operationQueue.get())
          .setDispatchedSize(dispatchedSize.get())
          .build();
    }
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public GetClientStartTimeResult getClientStartTime(GetClientStartTimeRequest request)
      throws IOException {
    List<GetClientStartTime> startTimes = new ArrayList<>();
    for (String key : request.getHostNameList()) {
      try {
        startTimes.add(
            client.call(
                jedis ->
                    GetClientStartTime.newBuilder()
                        .setInstanceName(key)
                        .setClientStartTime(Timestamps.fromMillis(Long.parseLong(jedis.get(key))))
                        .build()));
      } catch (NumberFormatException nfe) {
        log.warning("Could not obtain start time for " + key);
      }
    }
    return GetClientStartTimeResult.newBuilder().addAllClientStartTime(startTimes).build();
  }

  @Override
  public void updateDigestsExpiry(Iterable<Digest> digests) throws IOException {
    client.run(jedis -> createCasWorkerMap(jedis).setExpire(digests));
  }

  @Override
  public void indexCorrelatedInvocationsId(
      String correlatedInvocationsId, Map<String, List<String>> indexScopeValues)
      throws IOException {
    client.run(
        jedis -> {
          for (Map.Entry<String, List<String>> entry : indexScopeValues.entrySet()) {
            for (String key : entry.getValue()) {
              state.correlatedInvocationsIndex.add(
                  jedis, entry.getKey() + "=" + key, correlatedInvocationsId);
            }
          }
        });
  }

  @Override
  public void addToolInvocationId(
      String toolInvocationId, String correlatedInvocationsId, ToolDetails toolDetails)
      throws IOException {
    client.run(
        jedis -> {
          state.correlatedInvocations.add(jedis, correlatedInvocationsId, toolInvocationId);
          // TODO maybe index by toolDetails
        });
  }

  @Override
  public void incrementRequestCounters(
      String actionId, String toolInvocationId, String actionMnemonic, String targetId) {
    // TODO count for each of these fields
  }
}
