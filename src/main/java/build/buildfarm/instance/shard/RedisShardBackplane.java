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

package build.buildfarm.instance.shard;

import static redis.clients.jedis.ScanParams.SCAN_POINTER_START;
import static java.lang.String.format;
import static java.util.logging.Level.SEVERE;

import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecutionStage;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.ShardBackplane;
import build.buildfarm.common.Watcher;
import build.buildfarm.common.function.InterruptingRunnable;
import build.buildfarm.instance.shard.RedisShardSubscriber.TimedWatchFuture;
import build.buildfarm.v1test.CompletedOperationMetadata;
import build.buildfarm.v1test.DispatchedOperation;
import build.buildfarm.v1test.ExecuteEntry;
import build.buildfarm.v1test.ExecutingOperationMetadata;
import build.buildfarm.v1test.OperationChange;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.QueuedOperationMetadata;
import build.buildfarm.v1test.RedisShardBackplaneConfig;
import build.buildfarm.v1test.ShardWorker;
import build.buildfarm.v1test.WorkerChange;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.longrunning.Operation;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import com.google.rpc.PreconditionFailure;
import io.grpc.Status;
import io.grpc.Status.Code;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import javax.naming.ConfigurationException;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisClusterPipeline;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Response;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.exceptions.JedisNoReachableClusterNodeException;

public class RedisShardBackplane implements ShardBackplane {
  private static final Logger logger = Logger.getLogger(RedisShardBackplane.class.getName());

  private static final JsonFormat.Parser operationParser = JsonFormat.parser()
      .usingTypeRegistry(
          JsonFormat.TypeRegistry.newBuilder()
              .add(CompletedOperationMetadata.getDescriptor())
              .add(ExecutingOperationMetadata.getDescriptor())
              .add(ExecuteOperationMetadata.getDescriptor())
              .add(QueuedOperationMetadata.getDescriptor())
              .add(PreconditionFailure.getDescriptor())
              .build())
      .ignoringUnknownFields();

  static final JsonFormat.Printer operationPrinter = JsonFormat.printer().usingTypeRegistry(
      JsonFormat.TypeRegistry.newBuilder()
          .add(CompletedOperationMetadata.getDescriptor())
          .add(ExecutingOperationMetadata.getDescriptor())
          .add(ExecuteOperationMetadata.getDescriptor())
          .add(QueuedOperationMetadata.getDescriptor())
          .add(PreconditionFailure.getDescriptor())
          .build());

  private final RedisShardBackplaneConfig config;
  private final String source; // used in operation change publication
  private final Function<Operation, Operation> onPublish;
  private final Function<Operation, Operation> onComplete;
  private final Predicate<Operation> isPrequeued;
  private final Predicate<Operation> isDispatched;
  private final Supplier<JedisCluster> jedisClusterFactory;

  private @Nullable InterruptingRunnable onUnsubscribe = null;
  private Thread subscriptionThread = null;
  private Thread failsafeOperationThread = null;
  private RedisShardSubscriber subscriber = null;
  private RedisShardSubscription operationSubscription = null;
  private ExecutorService subscriberService = null;
  private boolean poolStarted = false;
  private @Nullable JedisCluster jedisCluster = null;

  private Set<String> workerSet = Collections.synchronizedSet(new HashSet<>());
  private long workerSetExpiresAt = 0;

  private static class JedisMisconfigurationException extends JedisDataException {
    public JedisMisconfigurationException(final String message) {
        super(message);
    }

    public JedisMisconfigurationException(final Throwable cause) {
        super(cause);
    }

    public JedisMisconfigurationException(final String message, final Throwable cause) {
        super(message, cause);
    }
  }

  private static JedisPoolConfig createJedisPoolConfig(RedisShardBackplaneConfig config) {
    JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
    jedisPoolConfig.setMaxTotal(config.getJedisPoolMaxTotal());
    return jedisPoolConfig;
  }

  private static URI parseRedisURI(String redisURI) throws ConfigurationException {
    try {
      return new URI(redisURI);
    } catch (URISyntaxException e) {
      throw new ConfigurationException(e.getMessage());
    }
  }

  private static Supplier<JedisCluster> createJedisClusterFactory(URI redisURI, JedisPoolConfig poolConfig) {
    return () -> new JedisCluster(
        new HostAndPort(redisURI.getHost(), redisURI.getPort()),
        poolConfig);
  }

  public RedisShardBackplane(
      RedisShardBackplaneConfig config,
      String source,
      Function<Operation, Operation> onPublish,
      Function<Operation, Operation> onComplete,
      Predicate<Operation> isPrequeued,
      Predicate<Operation> isDispatched) throws ConfigurationException {
    this(
        config,
        source,
        onPublish,
        onComplete,
        isPrequeued,
        isDispatched,
        createJedisClusterFactory(parseRedisURI(config.getRedisUri()), createJedisPoolConfig(config)));
  }

  public RedisShardBackplane(
      RedisShardBackplaneConfig config,
      String source,
      Function<Operation, Operation> onPublish,
      Function<Operation, Operation> onComplete,
      Predicate<Operation> isPrequeued,
      Predicate<Operation> isDispatched,
      Supplier<JedisCluster> jedisClusterFactory) {
    this.config = config;
    this.source = source;
    this.onPublish = onPublish;
    this.onComplete = onComplete;
    this.isPrequeued = isPrequeued;
    this.isDispatched = isDispatched;
    this.jedisClusterFactory = jedisClusterFactory;
  }

  @Override
  public InterruptingRunnable setOnUnsubscribe(InterruptingRunnable onUnsubscribe) {
    InterruptingRunnable oldOnUnsubscribe = this.onUnsubscribe;
    this.onUnsubscribe = onUnsubscribe;
    return oldOnUnsubscribe;
  }

  private Instant getExpiresAt(JedisCluster jedis, String key, Instant now) {
    String value = jedis.get(key);
    if (value != null) {
      try {
        return Instant.ofEpochMilli(Long.parseLong(value));
      } catch (NumberFormatException e) {
        logger.severe(format("invalid expiration %s for %s", value, key));
      }
    }

    Instant expiresAt = now.plusMillis(config.getProcessingTimeoutMillis());
    jedis.setex(
        key,
        /* expire=*/ (config.getProcessingTimeoutMillis() * 2) / 1000,
        String.format("%d", expiresAt.toEpochMilli()));
    return expiresAt;
  }

  abstract static class ListVisitor {
    private static final int LIST_PAGE_SIZE = 10000;

    protected abstract void visit(String entry);

    // this can potentially operate over the same set of entries in multiple steps
    public static void visit(JedisCluster jedis, String name, ListVisitor visitor) {
      int index = 0;
      int nextIndex = LIST_PAGE_SIZE;
      List<String> entries;
      do {
        entries = jedis.lrange(name, index, nextIndex - 1);
        for (String entry : entries) {
          visitor.visit(entry);
        }
        index = nextIndex;
        nextIndex += entries.size();
      } while (entries.size() == LIST_PAGE_SIZE);
    }
  }

  abstract static class QueueEntryListVisitor extends ListVisitor {
    protected abstract void visit(QueueEntry queueEntry, String queueEntryJson);

    @Override
    protected void visit(String entry) {
      QueueEntry.Builder queueEntry = QueueEntry.newBuilder();
      try {
        JsonFormat.parser().merge(entry, queueEntry);
        visit(queueEntry.build(), entry);
      } catch (InvalidProtocolBufferException e) {
        logger.log(SEVERE, "invalid QueueEntry json: " + entry, e);
      }
    }
  }

  abstract static class ExecuteEntryListVisitor extends ListVisitor {
    protected abstract void visit(ExecuteEntry executeEntry, String executeEntryJson);

    @Override
    protected void visit(String entry) {
      ExecuteEntry.Builder executeEntry = ExecuteEntry.newBuilder();
      try {
        JsonFormat.parser().merge(entry, executeEntry);
        visit(executeEntry.build(), entry);
      } catch (InvalidProtocolBufferException e) {
        logger.log(SEVERE, "invalid ExecuteEntry json: " + entry, e);
      }
    }
  }

  private void scanProcessing(JedisCluster jedis, Consumer<String> onOperationName, Instant now) {
    ListVisitor.visit(
        jedis,
        config.getProcessingListName(),
        new ExecuteEntryListVisitor() {
          @Override
          protected void visit(ExecuteEntry executeEntry, String executeEntryJson) {
            String operationName = executeEntry.getOperationName();
            String operationProcessingKey = processingKey(operationName);

            Instant expiresAt = getExpiresAt(jedis, operationProcessingKey, now);
            if (now.isBefore(expiresAt)) {
              onOperationName.accept(operationName);
            } else {
              if (jedis.lrem(config.getProcessingListName(), -1, executeEntryJson) != 0) {
                jedis.del(operationProcessingKey);
              }
            }
          }
        });
  }

  private void scanDispatching(JedisCluster jedis, Consumer<String> onOperationName, Instant now) {
    ListVisitor.visit(
        jedis,
        config.getDispatchingListName(),
        new QueueEntryListVisitor() {
          @Override
          protected void visit(QueueEntry queueEntry, String queueEntryJson) {
            String operationName = queueEntry.getExecuteEntry().getOperationName();
            String operationDispatchingKey = dispatchingKey(operationName);

            Instant expiresAt = getExpiresAt(jedis, operationDispatchingKey, now);
            if (now.isBefore(expiresAt)) {
              onOperationName.accept(operationName);
            } else {
              if (jedis.lrem(config.getDispatchingListName(), -1, queueEntryJson) != 0) {
                jedis.del(operationDispatchingKey);
              }
            }
          }
        });
  }

  private void scanPrequeue(JedisCluster jedis, Consumer<String> onOperationName) {
    ListVisitor.visit(
        jedis,
        config.getPreQueuedOperationsListName(),
        new ExecuteEntryListVisitor() {
          @Override
          protected void visit(ExecuteEntry executeEntry, String executeEntryJson) {
            onOperationName.accept(executeEntry.getOperationName());
          }
        });
  }

  private void scanQueue(JedisCluster jedis, Consumer<String> onOperationName) {
    ListVisitor.visit(
        jedis,
        config.getQueuedOperationsListName(),
        new QueueEntryListVisitor() {
          @Override
          protected void visit(QueueEntry queueEntry, String queueEntryJson) {
            onOperationName.accept(queueEntry.getExecuteEntry().getOperationName());
          }
        });
  }

  private void scanDispatched(JedisCluster jedis, Consumer<String> onOperationName) {
    for (String operationName : jedis.hkeys(config.getDispatchedOperationsHashName())) {
      onOperationName.accept(operationName);
    }
  }

  private void updateWatchers(JedisCluster jedis) {
    Instant now = Instant.now();
    Instant expiresAt = nextExpiresAt(now);
    Set<String> expiringChannels = Sets.newHashSet(
        subscriber.expiredWatchedOperationChannels(now));
    Consumer<String> resetChannel = (operationName) -> {
      String channel = operationChannel(operationName);
      if (expiringChannels.remove(channel)) {
        subscriber.resetWatchers(channel, expiresAt);
      }
    };

    if (!expiringChannels.isEmpty()) {
      logger.info(
          format(
              "Scan %d watches, %s, expiresAt: %s",
              expiringChannels.size(),
              now,
              expiresAt));

      logger.info("Scan prequeue");
      // scan prequeue, pet watches
      scanPrequeue(jedis, resetChannel);
    }

    // scan processing, create ttl key if missing, remove dead entries, pet live watches
    scanProcessing(jedis, resetChannel, now);

    if (!expiringChannels.isEmpty()) {
      logger.info("Scan queue");
      // scan queue, pet watches
      scanQueue(jedis, resetChannel);
    }

    // scan dispatching, create ttl key if missing, remove dead entries, pet live watches
    scanDispatching(jedis, resetChannel, now);

    if (!expiringChannels.isEmpty()) {
      logger.info("Scan dispatched");
      // scan dispatched pet watches
      scanDispatched(jedis, resetChannel);
    }

    //
    // filter watches on expiration
    // delete the operation?
    // update expired watches with null operation
    for (String channel : expiringChannels) {
      Operation operation = parseOperationJson(getOperation(jedis, parseOperationChannel(channel)));
      if (operation == null || !operation.getDone()) {
        publishExpiration(jedis, channel, now, /* force=*/ false);
      } else {
        subscriber.onOperation(
            channel,
            onPublish.apply(operation),
            expiresAt);
      }
    }
  }

  static String printOperationChange(OperationChange operationChange) throws InvalidProtocolBufferException {
    return operationPrinter.print(operationChange);
  }

  void publish(JedisCluster jedis, String channel, Instant effectiveAt, OperationChange.Builder operationChange) {
    try {
      String operationChangeJson = printOperationChange(
          operationChange
              .setEffectiveAt(toTimestamp(effectiveAt))
              .setSource(source)
              .build());
      jedis.publish(channel, operationChangeJson);
    } catch (InvalidProtocolBufferException e) {
      logger.log(SEVERE, "error printing operation change", e);
      // very unlikely, printer would have to fail
    }
  }

  void publishReset(JedisCluster jedis, Operation operation) {
    Instant effectiveAt = Instant.now();
    Instant expiresAt = nextExpiresAt(effectiveAt);
    publish(
        jedis,
        operationChannel(operation.getName()),
        Instant.now(),
        OperationChange.newBuilder()
            .setReset(OperationChange.Reset.newBuilder()
                .setExpiresAt(toTimestamp(expiresAt))
                .setOperation(operation)
                .build()));
  }

  static Timestamp toTimestamp(Instant instant) {
    return Timestamp.newBuilder()
        .setSeconds(instant.getEpochSecond())
        .setNanos(instant.getNano())
        .build();
  }

  void publishExpiration(JedisCluster jedis, String channel, Instant effectiveAt, boolean force) {
    publish(
        jedis,
        channel,
        effectiveAt,
        OperationChange.newBuilder()
            .setExpire(OperationChange.Expire.newBuilder()
                .setForce(force)
                .build()));
  }

  public void updateWatchedIfDone(JedisCluster jedis) {
    List<String> operationChannels = subscriber.watchedOperationChannels();
    if (operationChannels.isEmpty()) {
      return;
    }

    Instant now = Instant.now();
    List<Map.Entry<String, Response<String>>> operations = new ArrayList(operationChannels.size());
    JedisClusterPipeline p = jedis.pipelined();
    for (String operationName : Iterables.transform(operationChannels, RedisShardBackplane::parseOperationChannel)) {
      operations.add(new AbstractMap.SimpleEntry<>(
          operationName,
          p.get(operationKey(operationName))));
    }
    p.sync();

    for (Map.Entry<String, Response<String>> entry : operations) {
      String json = entry.getValue().get();
      Operation operation = json == null
          ? null : RedisShardBackplane.parseOperationJson(json);
      String operationName = entry.getKey();
      if (operation == null || operation.getDone()) {
        if (operation != null) {
          operation = onPublish.apply(operation);
        }
        subscriber.onOperation(
            operationChannel(operationName),
            operation,
            nextExpiresAt(now));
        logger.info(
            format(
                "operation %s done due to %s",
                operationName,
                operation == null ? "null" : "completed"));
      }
    }
  }

  private Instant nextExpiresAt(Instant from) {
    return from.plusSeconds(10);
  }

  private void startSubscriptionThread() {
    ListMultimap<String, TimedWatchFuture> watchers =
        Multimaps.<String, TimedWatchFuture>synchronizedListMultimap(
            MultimapBuilder.linkedHashKeys().arrayListValues().build());
    subscriberService = Executors.newFixedThreadPool(32);
    subscriber = new RedisShardSubscriber(watchers, workerSet, config.getWorkerChannel(), subscriberService);

    operationSubscription = new RedisShardSubscription(
        subscriber,
        /* onUnsubscribe=*/ () -> {
          subscriptionThread = null;
          if (onUnsubscribe != null) {
            onUnsubscribe.runInterruptibly();
          }
        },
        /* onReset=*/ this::updateWatchedIfDone,
        /* subscriptions=*/ subscriber::subscribedChannels,
        this::getSubscribeJedis);

    // use Executors...
    subscriptionThread = new Thread(operationSubscription);

    subscriptionThread.start();
  }

  private void startFailsafeOperationThread() {
    failsafeOperationThread = new Thread(() -> {
      while (true) {
        try {
          TimeUnit.SECONDS.sleep(10);
          updateWatchers(getJedis());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        } catch (Exception e) {
          logger.log(SEVERE, "error while updating watchers in failsafe", e);
        }
      }
    });

    failsafeOperationThread.start();
  }

  @Override
  public void start() {
    poolStarted = true;
    if (config.getSubscribeToBackplane()) {
      startSubscriptionThread();
    }
    if (config.getRunFailsafeOperation()) {
      startFailsafeOperationThread();
    }
    jedisCluster = jedisClusterFactory.get();
  }

  @Override
  public synchronized void stop() throws InterruptedException {
    if (failsafeOperationThread != null) {
      failsafeOperationThread.stop();
      failsafeOperationThread.join();
      logger.fine("failsafeOperationThread has been stopped");
    }
    if (operationSubscription != null) {
      operationSubscription.stop();
      if (subscriptionThread != null) {
        subscriptionThread.join();
      }
      logger.fine("subscriptionThread has been stopped");
    }
    if (subscriberService != null) {
      subscriberService.shutdown();
      subscriberService.awaitTermination(10, TimeUnit.SECONDS);
      logger.fine("subscriberService has been stopped");
    }
    if (jedisCluster != null) {
      poolStarted = false;
      jedisCluster.close();
      jedisCluster = null;
      logger.fine("pool has been closed");
    }
  }

  @Override
  public boolean isStopped() {
    return !poolStarted;
  }

  @Override
  public ListenableFuture<Void> watchOperation(
      String operationName,
      Watcher watcher) throws IOException {
    TimedWatcher timedWatcher = new TimedWatcher(nextExpiresAt(Instant.now())) {
      @Override
      public void observe(Operation operation) {
        watcher.observe(operation);
      }
    };
    return subscriber.watch(
        operationChannel(operationName),
        timedWatcher);
  }

  @Override
  public boolean addWorker(ShardWorker shardWorker) throws IOException {
    String json = JsonFormat.printer().print(shardWorker);
    String workerChangeJson = JsonFormat.printer().print(
        WorkerChange.newBuilder()
            .setEffectiveAt(toTimestamp(Instant.now()))
            .setName(shardWorker.getEndpoint())
            .setAdd(WorkerChange.Add.getDefaultInstance())
            .build());
    return withBackplaneException(
        (jedis) -> {
          // could rework with an hget to publish prior, but this seems adequate, and
          // we are the only guaranteed source
          if (jedis.hset(config.getWorkersHashName(), shardWorker.getEndpoint(), json) == 1) {
            jedis.publish(config.getWorkerChannel(), workerChangeJson);
            return true;
          }
          return false;
        });
  }

  private static final String MISCONF_RESPONSE = "MISCONF";

  @FunctionalInterface
  private static interface JedisContext<T> {
    T run(JedisCluster jedis) throws JedisException;
  }

  @VisibleForTesting
  public void withVoidBackplaneException(Consumer<JedisCluster> withJedis) throws IOException {
    withBackplaneException(new JedisContext<Void>() {
      @Override
      public Void run(JedisCluster jedis) throws JedisException {
        withJedis.accept(jedis);
        return null;
      }
    });
  }

  @VisibleForTesting
  public <T> T withBackplaneException(JedisContext<T> withJedis) throws IOException {
    try {
      try {
        return withJedis.run(getJedis());
      } catch (JedisDataException e) {
        if (e.getMessage().startsWith(MISCONF_RESPONSE)) {
          throw new JedisMisconfigurationException(e.getMessage());
        }
        throw e;
      }
    } catch (JedisMisconfigurationException e) {
      // the backplane is configured not to accept writes currently
      // as a result of an error. The error is meant to indicate
      // that substantial resources were unavailable.
      // we must throw an IOException which indicates as much
      // this looks simply to me like a good opportunity to use UNAVAILABLE
      // we are technically not at RESOURCE_EXHAUSTED, this is a
      // persistent state which can exist long past the error
      throw new IOException(Status.UNAVAILABLE.withCause(e).asRuntimeException());
    } catch (JedisNoReachableClusterNodeException e) {
      throw new IOException(Status.UNAVAILABLE.withCause(e).asRuntimeException());
    } catch (JedisConnectionException e) {
      if ((e.getMessage() != null && e.getMessage().equals("Unexpected end of stream."))
          || e.getCause() instanceof ConnectException) {
        throw new IOException(Status.UNAVAILABLE.withCause(e).asRuntimeException());
      }
      Throwable cause = e;
      Status status = Status.UNKNOWN;
      while (status.getCode() == Code.UNKNOWN && cause != null) {
        String message = cause.getMessage() == null ? "" : cause.getMessage();
        if ((cause instanceof SocketException && cause.getMessage().equals("Connection reset"))
            || cause instanceof ConnectException
            || message.equals("Unexpected end of stream.")) {
          status = Status.UNAVAILABLE;
        } else if (cause instanceof SocketTimeoutException) {
          status = Status.DEADLINE_EXCEEDED;
        } else if (cause instanceof IOException) {
          throw (IOException) cause;
        } else {
          cause = cause.getCause();
        }
      }
      throw new IOException(status.withCause(cause == null ? e : cause).asRuntimeException());
    }
  }

  private boolean removeWorkerAndPublish(JedisCluster jedis, String name, String changeJson) {
    if (jedis.hdel(config.getWorkersHashName(), name) == 1) {
      jedis.publish(config.getWorkerChannel(), changeJson);
      return true;
    }
    return false;
  }

  @Override
  public boolean removeWorker(String name, String reason) throws IOException {
    WorkerChange workerChange = WorkerChange.newBuilder()
        .setName(name)
        .setRemove(WorkerChange.Remove.newBuilder()
            .setSource(source)
            .setReason(reason)
            .build())
        .build();
    String workerChangeJson = JsonFormat.printer().print(workerChange);
    return subscriber.removeWorker(name) &&
        withBackplaneException((jedis) -> removeWorkerAndPublish(jedis, name, workerChangeJson));
  }

  @Override
  public synchronized Set<String> getWorkers() throws IOException {
    long now = System.currentTimeMillis();
    if (now < workerSetExpiresAt) {
      return workerSet;
    }

    synchronized (workerSet) {
      Set<String> newWorkerSet = withBackplaneException((jedis) -> fetchAndExpireWorkers(jedis, now));
      workerSet.clear();
      workerSet.addAll(newWorkerSet);
    }

    // fetch every 3 seconds
    workerSetExpiresAt = now + 3000;
    return workerSet;
  }

  private void removeInvalidWorkers(JedisCluster jedis, long testedAt, List<ShardWorker> workers) {
    if (!workers.isEmpty()) {
      for (ShardWorker worker : workers) {
        String name = worker.getEndpoint();
        String reason = format("registration expired at %d, tested at %d", worker.getExpireAt(), testedAt);
        WorkerChange workerChange = WorkerChange.newBuilder()
            .setEffectiveAt(toTimestamp(Instant.now()))
            .setName(name)
            .setRemove(WorkerChange.Remove.newBuilder()
                .setSource(source)
                .setReason(reason)
                .build())
            .build();
        try {
          String workerChangeJson = JsonFormat.printer().print(workerChange);
          removeWorkerAndPublish(jedis, name, workerChangeJson);
        } catch (InvalidProtocolBufferException e) {
          logger.log(SEVERE, "error printing workerChange", e);
        }
      }
    }
  }

  private Set<String> fetchAndExpireWorkers(JedisCluster jedis, long now) {
    Set<String> workers = Sets.newConcurrentHashSet();
    ImmutableList.Builder<ShardWorker> invalidWorkers = ImmutableList.builder();
    for (Map.Entry<String, String> entry : jedis.hgetAll(config.getWorkersHashName()).entrySet()) {
      String json = entry.getValue();
      String name = entry.getKey();
      try {
        if (json == null) {
          invalidWorkers.add(ShardWorker.newBuilder()
              .setEndpoint(name)
              .build());
        } else {
          ShardWorker.Builder builder = ShardWorker.newBuilder();
          JsonFormat.parser().merge(json, builder);
          ShardWorker worker = builder.build();
          if (worker.getExpireAt() <= now) {
            invalidWorkers.add(worker);
          } else {
            workers.add(worker.getEndpoint());
          }
        }
      } catch (InvalidProtocolBufferException e) {
        invalidWorkers.add(ShardWorker.newBuilder()
            .setEndpoint(name)
            .build());
      }
    }
    removeInvalidWorkers(jedis, now, invalidWorkers.build());
    return workers;
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

  @Override
  public ActionResult getActionResult(ActionKey actionKey) throws IOException {
    String json = withBackplaneException((jedis) -> jedis.get(acKey(actionKey)));
    if (json == null) {
      return null;
    }

    ActionResult actionResult = parseActionResult(json);
    if (actionResult == null) {
      withVoidBackplaneException((jedis) -> removeActionResult(jedis, actionKey));
    }
    return actionResult;
  }

  // we do this by action hash only, so that we can use RequestMetadata to filter
  @Override
  public void blacklistAction(String actionId) throws IOException {
    withVoidBackplaneException((jedis) -> jedis.setex(actionBlacklistKey(actionId), config.getActionBlacklistExpire(), ""));
  }

  @Override
  public void putActionResult(ActionKey actionKey, ActionResult actionResult)
      throws IOException {
    String json = JsonFormat.printer().print(actionResult);
    withVoidBackplaneException((jedis) -> jedis.setex(acKey(actionKey), config.getActionCacheExpire(), json));
  }

  private void removeActionResult(JedisCluster jedis, ActionKey actionKey) {
    jedis.del(acKey(actionKey));
  }

  @Override
  public void removeActionResult(ActionKey actionKey) throws IOException {
    withVoidBackplaneException((jedis) -> removeActionResult(jedis, actionKey));
  }

  @Override
  public void removeActionResults(Iterable<ActionKey> actionKeys) throws IOException {
    withVoidBackplaneException((jedis) -> {
      JedisClusterPipeline p = jedis.pipelined();
      for (ActionKey actionKey : actionKeys) {
        p.del(acKey(actionKey));
      }
      p.sync();
    });
  }

  @Override
  public ActionCacheScanResult scanActionCache(String scanToken, int count) throws IOException {
    final String jedisScanToken = scanToken == null ? SCAN_POINTER_START : scanToken;

    ImmutableList.Builder<Map.Entry<ActionKey, String>> results = new ImmutableList.Builder<>();

    ScanParams scanParams = new ScanParams()
        .match(config.getActionCachePrefix() + ":*")
        .count(count);

    String token = withBackplaneException((jedis) -> {
      ScanResult<String> scanResult = jedis.scan(jedisScanToken, scanParams);
      List<String> keyResults = scanResult.getResult();

      List<Response<String>> actionResults = new ArrayList<>(keyResults.size());
      JedisClusterPipeline p = jedis.pipelined();
      for (int i = 0; i < keyResults.size(); i++) {
        actionResults.add(p.get(keyResults.get(i)));
      }
      p.sync();
      for (int i = 0; i < keyResults.size(); i++) {
        String json = actionResults.get(i).get();
        if (json == null) {
          continue;
        }
        String key = keyResults.get(i);
        results.add(new AbstractMap.SimpleEntry<>(
            DigestUtil.asActionKey(DigestUtil.parseDigest(key.split(":")[1])),
            json));
      }
      String cursor = scanResult.getCursor();
      return cursor.equals(SCAN_POINTER_START) ? null : cursor;
    });
    return new ActionCacheScanResult(
        token,
        Iterables.transform(
            results.build(),
            (entry) -> new AbstractMap.SimpleEntry<>(
                entry.getKey(),
                parseActionResult(entry.getValue()))));
  }

  @Override
  public void adjustBlobLocations(Digest blobDigest, Set<String> addWorkers, Set<String> removeWorkers) throws IOException {
    String key = casKey(blobDigest);
    withVoidBackplaneException((jedis) -> {
      for (String workerName : addWorkers) {
        jedis.sadd(key, workerName);
      }
      for (String workerName : removeWorkers) {
        jedis.srem(key, workerName);
      }
      jedis.expire(key, config.getCasExpire());
    });
  }

  @Override
  public void addBlobLocation(Digest blobDigest, String workerName) throws IOException {
    String key = casKey(blobDigest);
    withVoidBackplaneException((jedis) -> {
      jedis.sadd(key, workerName);
      jedis.expire(key, config.getCasExpire());
    });
  }

  @Override
  public void addBlobsLocation(Iterable<Digest> blobDigests, String workerName)
      throws IOException {
    withVoidBackplaneException((jedis) -> {
      JedisClusterPipeline p = jedis.pipelined();
      for (Digest blobDigest : blobDigests) {
        String key = casKey(blobDigest);
        p.sadd(key, workerName);
        p.expire(key, config.getCasExpire());
      }
      p.sync();
    });
  }

  @Override
  public void removeBlobLocation(Digest blobDigest, String workerName) throws IOException {
    String key = casKey(blobDigest);
    withVoidBackplaneException((jedis) -> jedis.srem(key, workerName));
  }

  @Override
  public void removeBlobsLocation(Iterable<Digest> blobDigests, String workerName)
      throws IOException {
    withVoidBackplaneException((jedis) -> {
      JedisClusterPipeline p = jedis.pipelined();
      for (Digest blobDigest : blobDigests) {
        p.srem(casKey(blobDigest), workerName);
      }
      p.sync();
    });
  }

  @Override
  public String getBlobLocation(Digest blobDigest) throws IOException {
    return withBackplaneException((jedis) -> jedis.srandmember(casKey(blobDigest)));
  }

  @Override
  public Set<String> getBlobLocationSet(Digest blobDigest) throws IOException {
    return withBackplaneException((jedis) -> jedis.smembers(casKey(blobDigest)));
  }

  @Override
  public Map<Digest, Set<String>> getBlobDigestsWorkers(Iterable<Digest> blobDigests)
      throws IOException {
    // FIXME pipeline
    ImmutableMap.Builder<Digest, Set<String>> blobDigestsWorkers = new ImmutableMap.Builder<>();
    withVoidBackplaneException((jedis) -> {
      for (Digest blobDigest : blobDigests) {
        Set<String> workers = jedis.smembers(casKey(blobDigest));
        if (workers.isEmpty()) {
          continue;
        }
        blobDigestsWorkers.put(blobDigest, workers);
      }
    });
    return blobDigestsWorkers.build();
  }

  public static WorkerChange parseWorkerChange(String workerChangeJson) throws InvalidProtocolBufferException {
    WorkerChange.Builder workerChange = WorkerChange.newBuilder();
    JsonFormat.parser().merge(workerChangeJson, workerChange);
    return workerChange.build();
  }

  public static OperationChange parseOperationChange(String operationChangeJson) throws InvalidProtocolBufferException {
    OperationChange.Builder operationChange = OperationChange.newBuilder();
    operationParser.merge(operationChangeJson, operationChange);
    return operationChange.build();
  }

  public static Operation parseOperationJson(String operationJson) {
    if (operationJson == null) {
      return null;
    }
    try {
      Operation.Builder operationBuilder = Operation.newBuilder();
      operationParser.merge(operationJson, operationBuilder);
      return operationBuilder.build();
    } catch (InvalidProtocolBufferException e) {
      logger.log(SEVERE, "error parsing operation from " + operationJson, e);
      return null;
    }
  }

  private String getOperation(JedisCluster jedis, String operationName) {
    String json = jedis.get(operationKey(operationName));
    if (json == null) {
      return null;
    }
    return json;
  }

  @Override
  public Operation getOperation(String operationName) throws IOException {
    String json = withBackplaneException((jedis) -> getOperation(jedis, operationName));
    return parseOperationJson(json);
  }

  @Override
  public boolean putOperation(Operation operation, ExecutionStage.Value stage) throws IOException {
    // FIXME queue and prequeue should no longer be passed to here
    boolean prequeue = stage == ExecutionStage.Value.UNKNOWN && !operation.getDone();
    boolean queue = stage == ExecutionStage.Value.QUEUED;
    boolean complete = !queue && operation.getDone();
    boolean publish = !queue && stage != ExecutionStage.Value.UNKNOWN;

    if (complete) {
      // for filtering anything that shouldn't be stored
      operation = onComplete.apply(operation);
    }

    String json;
    try {
      json = operationPrinter.print(operation);
    } catch (InvalidProtocolBufferException e) {
      logger.log(SEVERE, "error printing operation " + operation.getName(), e);
      return false;
    }

    Operation publishOperation;
    if (publish) {
      publishOperation = onPublish.apply(operation);
    } else {
      publishOperation = null;
    }

    String name = operation.getName();
    withVoidBackplaneException((jedis) -> {
      jedis.setex(operationKey(name), config.getOperationExpire(), json);
      if (publishOperation != null) {
        publishReset(jedis, publishOperation);
      }
      if (complete) {
        completeOperation(jedis, name);
      }
    });
    return true;
  }

  private void queue(JedisCluster jedis, String operationName, String queueEntryJson) {
    if (jedis.hdel(config.getDispatchedOperationsHashName(), operationName) == 1) {
      logger.warning(format("removed dispatched operation %s", operationName));
    }
    jedis.lpush(config.getQueuedOperationsListName(), queueEntryJson);
  }

  @Override
  public void queue(QueueEntry queueEntry, Operation operation) throws IOException {
    String operationName = operation.getName();
    String operationJson = operationPrinter.print(operation);
    String queueEntryJson = JsonFormat.printer().print(queueEntry);
    Operation publishOperation = onPublish.apply(operation);
    withVoidBackplaneException((jedis) -> {
      jedis.setex(operationKey(operationName), config.getOperationExpire(), operationJson);
      queue(jedis, operation.getName(), queueEntryJson);
      publishReset(jedis, publishOperation);
    });
  }

  public Map<String, Operation> getOperationsMap() throws IOException {
    ImmutableMap.Builder<String, String> builder = new ImmutableMap.Builder<>();
    withVoidBackplaneException((jedis) -> {
      for (Map.Entry<String, String> entry : jedis.hgetAll(config.getDispatchedOperationsHashName()).entrySet()) {
        builder.put(entry.getKey(), entry.getValue());
      }
    });
    return Maps.transformValues(builder.build(), RedisShardBackplane::parseOperationJson);
  }

  @Override
  public Iterable<String> getOperations() throws IOException {
    throw new UnsupportedOperationException();
    /*
    return withVoidBackplaneException((jedis) -> {
      Iterable<String> dispatchedOperations = jedis.hkeys(config.getDispatchedOperationsHashName());
      Iterable<String> queuedOperations = jedis.lrange(config.getQueuedOperationsListName(), 0, -1);
      return Iterables.concat(queuedOperations, dispatchedOperations, getCompletedOperations(jedis));
    });
    */
  }

  @Override
  public ImmutableList<DispatchedOperation> getDispatchedOperations() throws IOException {
    ImmutableList.Builder<DispatchedOperation> builder = new ImmutableList.Builder<>();
    Map<String, String> dispatchedOperations = withBackplaneException((jedis) -> jedis.hgetAll(config.getDispatchedOperationsHashName()));
    ImmutableList.Builder<String> invalidOperationNames = new ImmutableList.Builder<>();
    boolean hasInvalid = false;
    // executor work queue?
    for (Map.Entry<String, String> entry : dispatchedOperations.entrySet()) {
      try {
        DispatchedOperation.Builder dispatchedOperationBuilder = DispatchedOperation.newBuilder();
        JsonFormat.parser().merge(entry.getValue(), dispatchedOperationBuilder);
        builder.add(dispatchedOperationBuilder.build());
      } catch (InvalidProtocolBufferException e) {
        logger.log(SEVERE, "RedisShardBackplane::getDispatchedOperations: removing invalid operation " + entry.getKey(), e);
        /* guess we don't want to spin on this */
        invalidOperationNames.add(entry.getKey());
        hasInvalid = true;
      }
    }

    if (hasInvalid) {
      withVoidBackplaneException((jedis) -> {
        JedisClusterPipeline p = jedis.pipelined();
        for (String invalidOperationName : invalidOperationNames.build()) {
          p.hdel(config.getDispatchedOperationsHashName(), invalidOperationName);
        }
        p.sync();
      });
    }
    return builder.build();
  }

  private ExecuteEntry deprequeueOperation(JedisCluster jedis) {
    String executeEntryJson;
    do {
      executeEntryJson = jedis.brpoplpush(
          config.getPreQueuedOperationsListName(),
          config.getProcessingListName(),
          1);
      if (Thread.currentThread().isInterrupted()) {
        return null;
      }
    } while (executeEntryJson == null);

    ExecuteEntry.Builder executeEntryBuilder = ExecuteEntry.newBuilder();
    try {
      JsonFormat.parser().merge(executeEntryJson, executeEntryBuilder);
      ExecuteEntry executeEntry = executeEntryBuilder.build();
      String operationName = executeEntry.getOperationName();

      Operation operation = keepaliveOperation(operationName);
      // publish so that watchers reset their timeout
      publishReset(jedis, operation);

      // destroy the processing entry and ttl
      if (jedis.lrem(config.getProcessingListName(), -1, executeEntryJson) == 0) {
        logger.severe(
            format(
                "could not remove %s from %s",
                operationName,
                config.getProcessingListName()));
        return null;
      }
      jedis.del(processingKey(operationName)); // may or may not exist
      return executeEntry;
    } catch (InvalidProtocolBufferException e) {
      logger.log(SEVERE, "error parsing execute entry", e);
      return null;
    }
  }

  @Override
  public ExecuteEntry deprequeueOperation() throws IOException, InterruptedException {
    ExecuteEntry executeEntry = withBackplaneException(this::deprequeueOperation);
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
    return executeEntry;
  }

  private QueueEntry dispatchOperation(JedisCluster jedis) {
    String queueEntryJson;
    do {
      queueEntryJson = jedis.brpoplpush(
          config.getQueuedOperationsListName(),
          config.getDispatchingListName(),
          1);
      // right here is an operation loss risk
      if (Thread.currentThread().isInterrupted()) {
        return null;
      }
    } while (queueEntryJson == null);

    QueueEntry.Builder queueEntryBuilder = QueueEntry.newBuilder();
    try {
      JsonFormat.parser().merge(queueEntryJson, queueEntryBuilder);
    } catch (InvalidProtocolBufferException e) {
      logger.log(SEVERE, "error parsing queue entry", e);
      return null;
    }
    QueueEntry queueEntry = queueEntryBuilder.build();

    String operationName = queueEntry.getExecuteEntry().getOperationName();
    Operation operation = keepaliveOperation(operationName);
    publishReset(jedis, operation);

    long requeueAt = System.currentTimeMillis() + 30 * 1000;
    DispatchedOperation o = DispatchedOperation.newBuilder()
        .setQueueEntry(queueEntry)
        .setRequeueAt(requeueAt)
        .build();
    boolean success = false;
    try {
      String dispatchedOperationJson = JsonFormat.printer().print(o);

      /* if the operation is already in the dispatch list, fail the dispatch */
      success = jedis.hsetnx(
          config.getDispatchedOperationsHashName(),
          operationName,
          dispatchedOperationJson) == 1;
    } catch (InvalidProtocolBufferException e) {
      logger.log(SEVERE, "error printing dispatched operation", e);
      // very unlikely, printer would have to fail
    }

    if (success) {
      if (jedis.lrem(config.getDispatchingListName(), -1, queueEntryJson) == 0) {
        logger.warning(
            format(
                "operation %s was missing in %s, may be orphaned",
                operationName,
                config.getDispatchingListName()));
      }
      jedis.del(dispatchingKey(operationName)); // may or may not exist
      return queueEntry;
    }
    return null;
  }

  @Override
  public QueueEntry dispatchOperation() throws IOException, InterruptedException {
    QueueEntry queueEntry = withBackplaneException(this::dispatchOperation);
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
    return queueEntry;
  }

  @Override
  public void rejectOperation(QueueEntry queueEntry) throws IOException {
    String operationName = queueEntry.getExecuteEntry().getOperationName();
    String queueEntryJson = JsonFormat.printer().print(queueEntry);
    withVoidBackplaneException((jedis) -> {
      if (jedis.hdel(config.getDispatchedOperationsHashName(), operationName) == 1) {
        jedis.rpush(config.getQueuedOperationsListName(), queueEntryJson);
      }
    });
  }

  @Override
  public boolean pollOperation(QueueEntry queueEntry, ExecutionStage.Value stage, long requeueAt) throws IOException {
    String operationName = queueEntry.getExecuteEntry().getOperationName();
    DispatchedOperation o = DispatchedOperation.newBuilder()
        .setQueueEntry(queueEntry)
        .setRequeueAt(requeueAt)
        .build();
    String json;
    try {
      json = JsonFormat.printer().print(o);
    } catch (InvalidProtocolBufferException e) {
      logger.log(SEVERE, "error printing dispatched operation " + operationName, e);
      return false;
    }
    return withBackplaneException((jedis) -> {
      if (jedis.hexists(config.getDispatchedOperationsHashName(), operationName)) {
        if (jedis.hset(config.getDispatchedOperationsHashName(), operationName, json) == 0) {
          return true;
        }
        /* someone else beat us to the punch, delete our incorrectly added key */
        jedis.hdel(config.getDispatchedOperationsHashName(), operationName);
      }
      return false;
    });
  }

  @Override
  public void prequeue(ExecuteEntry executeEntry, Operation operation) throws IOException {
    String operationName = operation.getName();
    String operationJson = operationPrinter.print(operation);
    String executeEntryJson = JsonFormat.printer().print(executeEntry);
    Operation publishOperation = onPublish.apply(operation);
    withVoidBackplaneException((jedis) -> {
      jedis.setex(operationKey(operationName), config.getOperationExpire(), operationJson);
      jedis.lpush(config.getPreQueuedOperationsListName(), executeEntryJson);
      publishReset(jedis, publishOperation);
    });
  }

  private Operation keepaliveOperation(String operationName) {
    return Operation.newBuilder()
        .setName(operationName)
        .build();
  }

  @Override
  public void queueing(String operationName) throws IOException {
    Operation operation = keepaliveOperation(operationName);
    // publish so that watchers reset their timeout
    withVoidBackplaneException((jedis) -> {
      publishReset(jedis, operation);
    });
  }

  @Override
  public void requeueDispatchedOperation(QueueEntry queueEntry) throws IOException {
    String queueEntryJson = JsonFormat.printer().print(queueEntry);
    String operationName = queueEntry.getExecuteEntry().getOperationName();
    Operation publishOperation = keepaliveOperation(operationName);
    withVoidBackplaneException((jedis) -> {
      queue(jedis, operationName, queueEntryJson);
      publishReset(jedis, publishOperation);
    });
  }

  private void completeOperation(JedisCluster jedis, String operationName) {
    jedis.hdel(config.getDispatchedOperationsHashName(), operationName);
  }

  @Override
  public void completeOperation(String operationName) throws IOException {
    withVoidBackplaneException((jedis) -> completeOperation(jedis, operationName));
  }

  @Override
  public void deleteOperation(String operationName) throws IOException {
    Operation o = Operation.newBuilder()
        .setName(operationName)
        .setDone(true)
        .setError(com.google.rpc.Status.newBuilder()
            .setCode(Code.UNAVAILABLE.value())
            .build())
        .build();

    withVoidBackplaneException((jedis) -> {
      completeOperation(jedis, operationName);
      // FIXME find a way to get rid of this thing from the queue by name
      // jedis.lrem(config.getQueuedOperationsListName(), 0, operationName);
      jedis.del(operationKey(operationName));

      publishReset(jedis, o);
    });
  }

  private JedisCluster getSubscribeJedis() throws IOException {
    if (!poolStarted) {
      throw new IOException(
          Status.UNAVAILABLE.withDescription("backend is not started").asRuntimeException());
    }
    return jedisClusterFactory.get();
  }

  private JedisCluster getJedis() throws IOException {
    if (!poolStarted) {
      throw new IOException(
          Status.UNAVAILABLE.withDescription("backend is not started").asRuntimeException());
    }
    return jedisCluster;
  }

  private String casKey(Digest blobDigest) {
    return config.getCasPrefix() + ":" + DigestUtil.toString(blobDigest);
  }

  private String acKey(ActionKey actionKey) {
    return config.getActionCachePrefix() + ":" + DigestUtil.toString(actionKey.getDigest());
  }

  String operationKey(String operationName) {
    return config.getOperationPrefix() + ":" + operationName;
  }

  String operationChannel(String operationName) {
    return config.getOperationChannelPrefix() + ":" + operationName;
  }

  private String processingKey(String operationName) {
    return config.getProcessingPrefix() + ":" + operationName;
  }

  private String dispatchingKey(String operationName) {
    return config.getDispatchingPrefix() + ":" + operationName;
  }

  public String actionBlacklistKey(String actionId) {
    return config.getActionBlacklistPrefix() + ":" + actionId;
  }

  public static String parseOperationChannel(String channel) {
    return channel.split(":")[1];
  }

  @Override
  public boolean isBlacklisted(RequestMetadata requestMetadata) throws IOException {
    // TODO build blacklisting?
    if (requestMetadata.getActionId().isEmpty()) {
      return false;
    }
    return withBackplaneException((jedis) -> jedis.exists(actionBlacklistKey(requestMetadata.getActionId())));
  }

  @Override
  public boolean canQueue() throws IOException {
    int maxQueueDepth = config.getMaxQueueDepth();
    return maxQueueDepth < 0
        || withBackplaneException((jedis) -> jedis.llen(config.getQueuedOperationsListName()) < maxQueueDepth);
  }

  @Override
  public boolean canPrequeue() throws IOException {
    int maxPreQueueDepth = config.getMaxPreQueueDepth();
    return maxPreQueueDepth < 0
        || withBackplaneException((jedis) -> jedis.llen(config.getPreQueuedOperationsListName()) < maxPreQueueDepth);
  }
}
