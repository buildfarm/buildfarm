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

import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.ShardBackplane;
import build.buildfarm.v1test.CompletedOperationMetadata;
import build.buildfarm.v1test.QueuedOperationMetadata;
import build.buildfarm.v1test.RedisShardBackplaneConfig;
import build.buildfarm.v1test.ShardDispatchedOperation;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.devtools.remoteexecution.v1test.ActionResult;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.Directory;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata.Stage;
import com.google.devtools.remoteexecution.v1test.GetTreeResponse;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Status;
import io.grpc.Status.Code;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.naming.ConfigurationException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.Pool;

public class RedisShardBackplane implements ShardBackplane {
  private final RedisShardBackplaneConfig config;
  private final Function<Operation, Operation> onPublish;
  private final Function<Operation, Operation> onComplete;
  private final Runnable onUnsubscribe;
  private final Pool<Jedis> pool;

  private Thread subscriptionThread = null;
  private Thread failsafeOperationThread = null;
  private OperationSubscriber operationSubscriber = null;
  private RedisShardSubscription operationSubscription = null;
  private boolean poolStarted = false;

  private Set<String> workerSet = null;
  private long workerSetExpiresAt = 0;

  private static final JsonFormat.Printer operationPrinter = JsonFormat.printer().usingTypeRegistry(
      JsonFormat.TypeRegistry.newBuilder()
          .add(CompletedOperationMetadata.getDescriptor())
          .add(ExecuteOperationMetadata.getDescriptor())
          .add(QueuedOperationMetadata.getDescriptor())
          .build());

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

  public RedisShardBackplane(
      RedisShardBackplaneConfig config,
      Function<Operation, Operation> onPublish,
      Function<Operation, Operation> onComplete,
      Runnable onUnsubscribe) throws ConfigurationException {
    this(
        config,
        onPublish,
        onComplete,
        onUnsubscribe,
        new JedisPool(createJedisPoolConfig(config), parseRedisURI(config.getRedisUri()), /* connectionTimeout=*/ 30000, /* soTimeout=*/ 30000));
  }

  public RedisShardBackplane(
      RedisShardBackplaneConfig config,
      Function<Operation, Operation> onPublish,
      Function<Operation, Operation> onComplete,
      Runnable onUnsubscribe,
      JedisPool pool) {
    this.config = config;
    this.onPublish = onPublish;
    this.onComplete = onComplete;
    this.onUnsubscribe = onUnsubscribe;
    this.pool = pool;
  }

  public void updateWatchedIfDone(Jedis jedis) {
    List<String> operationChannels = operationSubscriber.watchedOperationChannels();
    if (operationChannels.isEmpty()) {
      return;
    }

    System.out.println("RedisShardBackplane::updateWatchedIfDone: Checking on open watches");

    List<Map.Entry<String, Response<String>>> operations = new ArrayList(operationChannels.size());
    Pipeline p = jedis.pipelined();
    for (String operationName : Iterables.transform(operationChannels, RedisShardBackplane::parseOperationChannel)) {
      operations.add(new AbstractMap.SimpleEntry<>(
          operationName,
          p.get(operationKey(operationName))));
    }
    p.sync();

    int iRemainingIncomplete = 20;
    for (Map.Entry<String, Response<String>> entry : operations) {
      String json = entry.getValue().get();
      Operation operation = json == null
          ? null : RedisShardBackplane.parseOperationJson(json);
      String operationName = entry.getKey();
      if (operation == null || operation.getDone()) {
        System.out.println("RedisShardBackplane::updateWatchedIfDone: Operation " + operationName + " done due to " + (operation == null ? "null" : "completed"));
        operationSubscriber.onOperation(operationChannel(operationName), operation);
      } else if (iRemainingIncomplete > 0) {
        System.out.println("RedisShardBackplane::updateWatchedIfDone: Operation " + operationName);
        iRemainingIncomplete--;
      }
    }
  }

  private void startSubscriptionThread() {
    operationSubscriber = new OperationSubscriber();

    operationSubscription = new RedisShardSubscription(
        operationSubscriber,
        /* onUnsubscribe=*/ () -> {
          subscriptionThread = null;
          onUnsubscribe.run();
        },
        /* onReset=*/ this::updateWatchedIfDone,
        /* subscriptions=*/ operationSubscriber::watchedOperationChannels,
        this::getJedis);

    // use Executors...
    subscriptionThread = new Thread(operationSubscription);

    subscriptionThread.start();

    failsafeOperationThread = new Thread(() -> {
      while (true) {
        try {
          TimeUnit.SECONDS.sleep(30);
          try (Jedis jedis = getJedis()) {
            updateWatchedIfDone(jedis);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });

    failsafeOperationThread.start();
  }

  @Override
  public void start() {
    poolStarted = true;
    if (config.getSubscribeToOperation()) {
      startSubscriptionThread();
    }
  }

  @Override
  public void stop() {
    if (subscriptionThread != null) {
      subscriptionThread.stop();
      // subscriptionThread.join();
      failsafeOperationThread.stop();
      // failsafeOperationThread.join();
    }
    if (pool != null) {
      poolStarted = false;
      pool.close();
    }
  }

  @Override
  public boolean watchOperation(String operationName, Predicate<Operation> watcher) throws IOException {
    operationSubscriber.watch(operationChannel(operationName), watcher);
    return true;
  }

  @Override
  public void addWorker(String workerName) throws IOException {
    withVoidBackplaneException((jedis) -> jedis.sadd(config.getWorkersSetName(), workerName));
  }

  private static final String MISCONF_RESPONSE = "MISCONF";

  @FunctionalInterface
  private static interface JedisContext<T> {
    T run(Jedis jedis) throws JedisException;
  }

  @VisibleForTesting
  public void withVoidBackplaneException(Consumer<Jedis> withJedis) throws IOException {
    withBackplaneException(new JedisContext<Void>() {
      @Override
      public Void run(Jedis jedis) throws JedisException {
        withJedis.accept(jedis);
        return null;
      }
    });
  }

  @VisibleForTesting
  public <T> T withBackplaneException(JedisContext<T> withJedis) throws IOException {
    try (Jedis jedis = getJedis()) {
      try {
        return withJedis.run(jedis);
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
      // this looks simply to me like a good opportunity to use FAILED_PRECONDITION
      // we are technically not at RESOURCE_EXHAUSTED, this is a
      // persistent state which can exist long past the error
      throw new IOException(Status.FAILED_PRECONDITION.withCause(e).asRuntimeException());
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
        }
        cause = cause.getCause();
      }
      if (cause != null) {
        status = status.withCause(cause);
      }
      throw new IOException(status.asRuntimeException());
    }
  }

  @Override
  public void removeWorker(String workerName) throws IOException {
    if (workerSet != null) {
      workerSet.remove(workerName);
    }

    withVoidBackplaneException((jedis) -> jedis.srem(config.getWorkersSetName(), workerName));
  }

  @Override
  public String getRandomWorker() throws IOException {
    return withBackplaneException((jedis) -> jedis.srandmember(config.getWorkersSetName()));
  }

  @Override
  public Set<String> getWorkerSet() throws IOException {
    if (System.nanoTime() < workerSetExpiresAt) {
      return workerSet;
    }

    workerSet = withBackplaneException((jedis) -> jedis.smembers(config.getWorkersSetName()));

    // fetch every 3 seconds
    workerSetExpiresAt = System.nanoTime() + 3000000000l;
    return workerSet;
  }

  @Override
  public boolean isWorker(String workerName) throws IOException {
    return withBackplaneException((jedis) -> jedis.sismember(config.getWorkersSetName(), workerName));
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

  @Override
  public void putActionResult(ActionKey actionKey, ActionResult actionResult)
      throws IOException {
    String json = JsonFormat.printer().print(actionResult);
    withVoidBackplaneException((jedis) -> jedis.setex(acKey(actionKey), config.getActionCacheExpire(), json));
  }

  private void removeActionResult(Jedis jedis, ActionKey actionKey) {
    jedis.del(acKey(actionKey));
  }

  @Override
  public void removeActionResult(ActionKey actionKey) throws IOException {
    withVoidBackplaneException((jedis) -> removeActionResult(jedis, actionKey));
  }

  @Override
  public void removeActionResults(Iterable<ActionKey> actionKeys) throws IOException {
    withVoidBackplaneException((jedis) -> {
      Pipeline p = jedis.pipelined();
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
      Pipeline p = jedis.pipelined();
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
      return scanResult.getStringCursor().equals(SCAN_POINTER_START) ? null : scanResult.getStringCursor();
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
  public void addBlobLocation(Digest blobDigest, String workerName) throws IOException {
    String key = casKey(blobDigest);
    withVoidBackplaneException((jedis) -> {
      Transaction t = jedis.multi();
      t.sadd(key, workerName);
      t.expire(key, config.getCasExpire());
      t.exec();
    });
  }

  @Override
  public void addBlobsLocation(Iterable<Digest> blobDigests, String workerName)
      throws IOException {
    withVoidBackplaneException((jedis) -> {
      Pipeline p = jedis.pipelined();
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
      Pipeline p = jedis.pipelined();
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

  public static Operation parseOperationJson(String operationJson) {
    if (operationJson == null) {
      return null;
    }
    try {
      Operation.Builder operationBuilder = Operation.newBuilder();
      getOperationParser().merge(operationJson, operationBuilder);
      return operationBuilder.build();
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
      return null;
    }
  }

  private static JsonFormat.Parser getOperationParser() {
    return JsonFormat.parser().usingTypeRegistry(
        JsonFormat.TypeRegistry.newBuilder()
            .add(CompletedOperationMetadata.getDescriptor())
            .add(ExecuteOperationMetadata.getDescriptor())
            .add(QueuedOperationMetadata.getDescriptor())
            .build());
  }

  private String getOperation(Jedis jedis, String operationName) {
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
  public boolean putOperation(Operation operation, Stage stage) throws IOException {
    boolean queue = stage == Stage.QUEUED;
    boolean complete = !queue && operation.getDone();
    boolean publish = !queue && stage != Stage.UNKNOWN;

    if (complete) {
      // for filtering anything that shouldn't be stored
      operation = onComplete.apply(operation);
    }

    String json;
    try {
      json = operationPrinter.print(operation);
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
      return false;
    }

    String publishOperation;
    if (publish) {
      publishOperation = operationPrinter.print(onPublish.apply(operation));
    } else {
      publishOperation = null;
    }

    String name = operation.getName();
    withVoidBackplaneException((jedis) -> {
      if (complete) {
        completeOperation(jedis, name);
      }
      jedis.setex(operationKey(name), config.getOperationExpire(), json);
      if (queue) {
        queueOperation(jedis, name);
      }
      if (publish) {
        jedis.publish(operationChannel(name), publishOperation);
      }
    });
    return true;
  }

  private void queueOperation(Jedis jedis, String operationName) {
    if (jedis.hdel(config.getDispatchedOperationsHashName(), operationName) == 1) {
      System.err.println("RedisShardBackplane::queueOperation: WARNING Removed dispatched operation");
    }
    jedis.lpush(config.getQueuedOperationsListName(), operationName);
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
  public ImmutableList<ShardDispatchedOperation> getDispatchedOperations() throws IOException {
    ImmutableList.Builder<ShardDispatchedOperation> builder = new ImmutableList.Builder<>();
    Map<String, String> dispatchedOperations = withBackplaneException((jedis) -> jedis.hgetAll(config.getDispatchedOperationsHashName()));
    ImmutableList.Builder<String> invalidOperationNames = new ImmutableList.Builder<>();
    boolean hasInvalid = false;
    // executor work queue?
    for (Map.Entry<String, String> entry : dispatchedOperations.entrySet()) {
      try {
        ShardDispatchedOperation.Builder dispatchedOperationBuilder = ShardDispatchedOperation.newBuilder();
        JsonFormat.parser().merge(entry.getValue(), dispatchedOperationBuilder);
        builder.add(dispatchedOperationBuilder.build());
      } catch (InvalidProtocolBufferException e) {
        System.err.println("RedisShardBackplane::getDispatchedOperations: removing invalid operation " + entry.getKey());
        e.printStackTrace();
        /* guess we don't want to spin on this */
        invalidOperationNames.add(entry.getKey());
        hasInvalid = true;
      }
    }

    if (hasInvalid) {
      withVoidBackplaneException((jedis) -> {
        Pipeline p = jedis.pipelined();
        for (String invalidOperationName : invalidOperationNames.build()) {
          p.hdel(config.getDispatchedOperationsHashName(), invalidOperationName);
        }
        p.sync();
      });
    }
    return builder.build();
  }

  @Override
  public String dispatchOperation() throws IOException, InterruptedException {
    String operationName = withBackplaneException((jedis) -> {
      List<String> result;
      do {
        /* maybe we should really have a dispatch queue per registered worker */
        result = jedis.brpop(1, config.getQueuedOperationsListName());
        if (Thread.currentThread().isInterrupted()) {
          return null;
        }
      } while (result == null);

      if (result.size() == 2 && result.get(0).equals(config.getQueuedOperationsListName())) {
        return result.get(1);
      }
      return null;
    });

    if (operationName == null) {
      if (Thread.interrupted()) {
        throw new InterruptedException();
      }
      return null;
    }

    // right here is an operation loss risk

    ShardDispatchedOperation o = ShardDispatchedOperation.newBuilder()
        .setName(operationName)
        .setRequeueAt(System.currentTimeMillis() + 30 * 1000)
        .build();
    String json;
    try {
      json = JsonFormat.printer().print(o);
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
      return null;
    }
    /* if the operation is already in the dispatch list, don't requeue */
    boolean dispatched = withBackplaneException((jedis) -> jedis.hsetnx(config.getDispatchedOperationsHashName(), operationName, json) == 1);
    if (!dispatched) {
      return null;
    }

    return operationName;
  }

  @Override
  public boolean pollOperation(String operationName, Stage stage, long requeueAt) throws IOException {
    ShardDispatchedOperation o = ShardDispatchedOperation.newBuilder()
        .setName(operationName)
        .setRequeueAt(requeueAt)
        .build();
    String json;
    try {
      json = JsonFormat.printer().print(o);
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
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
  public void requeueDispatchedOperation(Operation operation) throws IOException {
    withVoidBackplaneException((jedis) -> queueOperation(jedis, operation.getName()));
  }

  private void completeOperation(Jedis jedis, String operationName) {
    if (jedis.hdel(config.getDispatchedOperationsHashName(), operationName) != 1) {
      System.err.println("RedisShardBackplane::completeOperation: WARNING " + operationName + " was not in dispatched list");
    }
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
    String json;
    try {
      json = JsonFormat.printer().print(o);
    } catch (InvalidProtocolBufferException e) {
      json = null;
      e.printStackTrace();
    }

    final String publishOperation = json;
    withVoidBackplaneException((jedis) -> {
      Transaction t = jedis.multi();
      t.hdel(config.getDispatchedOperationsHashName(), operationName);
      t.lrem(config.getQueuedOperationsListName(), 0, operationName);
      t.del(operationKey(operationName));
      t.exec();

      jedis.publish(operationChannel(operationName), publishOperation);
    });
  }

  private Jedis getJedis() {
    if (!poolStarted) {
      throw Status.FAILED_PRECONDITION.withDescription("pool is not started").asRuntimeException();
    }
    return pool.getResource();
  }

  private String casKey(Digest blobDigest) {
    return config.getCasPrefix() + ":" + DigestUtil.toString(blobDigest);
  }

  private String treeKey(Digest blobDigest) {
    return config.getTreePrefix() + ":" + DigestUtil.toString(blobDigest);
  }

  private String acKey(ActionKey actionKey) {
    return config.getActionCachePrefix() + ":" + DigestUtil.toString(actionKey.getDigest());
  }

  private String operationKey(String operationName) {
    return config.getOperationPrefix() + ":" + operationName;
  }

  private String operationChannel(String operationName) {
    return config.getOperationChannelPrefix() + ":" + operationName;
  }

  public static String parseOperationChannel(String channel) {
    return channel.split(":")[1];
  }

  @Override
  public void putTree(Digest inputRoot, Iterable<Directory> directories) throws IOException {
    String treeValue = JsonFormat.printer().print(GetTreeResponse.newBuilder()
        .addAllDirectories(directories)
        .build());
    withVoidBackplaneException((jedis) -> jedis.setex(treeKey(inputRoot), config.getTreeExpire(), treeValue));
  }

  @Override
  public Iterable<Directory> getTree(Digest inputRoot) throws IOException {
    String json = withBackplaneException((jedis) -> jedis.get(treeKey(inputRoot)));
    if (json == null) {
      return null;
    }

    try {
      GetTreeResponse.Builder builder = GetTreeResponse.newBuilder();
      JsonFormat.parser().merge(json, builder);
      return builder.build().getDirectoriesList();
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public void removeTree(Digest inputRoot) throws IOException {
    withVoidBackplaneException((jedis) -> jedis.del(treeKey(inputRoot)));
  }
}
