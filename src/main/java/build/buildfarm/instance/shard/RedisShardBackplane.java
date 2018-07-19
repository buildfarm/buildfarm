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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
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
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
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

public class RedisShardBackplane implements ShardBackplane {
  private final RedisShardBackplaneConfig config;
  private final Function<Operation, Operation> onPublish;
  private final Function<Operation, Operation> onComplete;
  private final Runnable onUnsubscribe;
  private final URI redisURI;

  private Thread subscriptionThread = null;
  private Thread failsafeOperationThread = null;
  private OperationSubscriber operationSubscriber = null;
  private RedisShardSubscription operationSubscription = null;
  private JedisPool pool = null;

  private Set<String> workerSet = null;
  private long workerSetExpiresAt = 0;

  private static final JsonFormat.Printer operationPrinter = JsonFormat.printer().usingTypeRegistry(
      JsonFormat.TypeRegistry.newBuilder()
          .add(CompletedOperationMetadata.getDescriptor())
          .add(ExecuteOperationMetadata.getDescriptor())
          .add(QueuedOperationMetadata.getDescriptor())
          .build());


  public RedisShardBackplane(
      RedisShardBackplaneConfig config,
      Function<Operation, Operation> onPublish,
      Function<Operation, Operation> onComplete,
      Runnable onUnsubscribe) throws ConfigurationException {
    this.config = config;
    this.onPublish = onPublish;
    this.onComplete = onComplete;
    this.onUnsubscribe = onUnsubscribe;

    try {
      redisURI = new URI(config.getRedisUri());
    } catch (URISyntaxException e) {
      throw new ConfigurationException(e.getMessage());
    }
  }

  private void startPool() {
    JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
    jedisPoolConfig.setMaxTotal(config.getJedisPoolMaxTotal());
    // no explicit start, has to be object lifetime...
    pool = new JedisPool(jedisPoolConfig, redisURI, /* connectionTimeout=*/ 30000, /* soTimeout=*/ 30000);
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
    startPool();
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
    try (Jedis jedis = getJedis()) {
      jedis.sadd(config.getWorkersSetName(), workerName);
    } catch (JedisConnectionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(e);
    }
  }

  @Override
  public void removeWorker(String workerName) throws IOException {
    if (workerSet != null) {
      workerSet.remove(workerName);
    }

    try (Jedis jedis = getJedis()) {
      jedis.srem(config.getWorkersSetName(), workerName);
    } catch (JedisConnectionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(e);
    }
  }

  @Override
  public String getRandomWorker() throws IOException {
    try (Jedis jedis = getJedis()) {
      return jedis.srandmember(config.getWorkersSetName());
    } catch (JedisConnectionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(e);
    }
  }

  @Override
  public Set<String> getWorkerSet() throws IOException {
    if (System.nanoTime() < workerSetExpiresAt) {
      return workerSet;
    }

    try (Jedis jedis = getJedis()) {
      workerSet = jedis.smembers(config.getWorkersSetName());
    } catch (JedisConnectionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(e);
    }

    // fetch every 3 seconds
    workerSetExpiresAt = System.nanoTime() + 3000000000l;
    return workerSet;
  }

  @Override
  public boolean isWorker(String workerName) throws IOException {
    try (Jedis jedis = getJedis()) {
      return jedis.sismember(config.getWorkersSetName(), workerName);
    } catch (JedisConnectionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(e);
    }
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
    try (Jedis jedis = getJedis()) {
      String json = jedis.get(acKey(actionKey));
      if (json == null) {
        return null;
      }

      ActionResult actionResult = parseActionResult(json);
      if (actionResult == null) {
        removeActionResult(jedis, actionKey);
      }
      return actionResult;
    } catch (JedisConnectionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(e);
    }
  }

  @Override
  public void putActionResult(ActionKey actionKey, ActionResult actionResult)
      throws IOException {
    try (Jedis jedis = getJedis()) {
      String json = JsonFormat.printer().print(actionResult);
      jedis.setex(acKey(actionKey), config.getActionCacheExpire(), json);
    } catch (JedisConnectionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(e);
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
    }
  }

  private void removeActionResult(Jedis jedis, ActionKey actionKey) {
    jedis.del(acKey(actionKey));
  }

  @Override
  public void removeActionResult(ActionKey actionKey) throws IOException {
    try (Jedis jedis = getJedis()) {
      removeActionResult(jedis, actionKey);
    } catch (JedisConnectionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(e);
    }
  }

  @Override
  public void removeActionResults(Iterable<ActionKey> actionKeys) throws IOException {
    try (Jedis jedis = getJedis()) {
      Pipeline p = jedis.pipelined();
      for (ActionKey actionKey : actionKeys) {
        removeActionResult(jedis, actionKey);
      }
      p.sync();
    } catch (JedisConnectionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(e);
    }
  }

  @Override
  public ActionCacheScanResult scanActionCache(String scanToken, int count) throws IOException {
    if (scanToken == null) {
      scanToken = SCAN_POINTER_START;
    }

    final String token;
    ImmutableList.Builder<Map.Entry<ActionKey, ActionResult>> results = new ImmutableList.Builder<>();

    ScanParams scanParams = new ScanParams()
        .match(config.getActionCachePrefix() + ":*")
        .count(count);

    try (Jedis jedis = getJedis()) {
      ScanResult<String> scanResult = jedis.scan(scanToken, scanParams);
      token = scanResult.getStringCursor().equals(SCAN_POINTER_START) ? null : scanResult.getStringCursor();
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
            parseActionResult(json)));
      }
    } catch (JedisConnectionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(e);
    }
    return new ActionCacheScanResult(
        token,
        results.build());
  }

  @Override
  public void addBlobLocation(Digest blobDigest, String workerName) throws IOException {
    try (Jedis jedis = getJedis()) {
      String key = casKey(blobDigest);
      Transaction t = jedis.multi();
      t.sadd(key, workerName);
      t.expire(key, config.getCasExpire());
      t.exec();
    } catch (JedisConnectionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(e);
    }
  }

  @Override
  public void addBlobsLocation(Iterable<Digest> blobDigests, String workerName)
      throws IOException {
    try (Jedis jedis = getJedis()) {
      Pipeline p = jedis.pipelined();
      for (Digest blobDigest : blobDigests) {
        String key = casKey(blobDigest);
        p.sadd(key, workerName);
        p.expire(key, config.getCasExpire());
      }
      p.sync();
    } catch (JedisConnectionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(e);
    }
  }

  @Override
  public void removeBlobLocation(Digest blobDigest, String workerName) throws IOException {
    try (Jedis jedis = getJedis()) {
      jedis.srem(casKey(blobDigest), workerName);
    } catch (JedisConnectionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(e);
    }
  }

  @Override
  public void removeBlobsLocation(Iterable<Digest> blobDigests, String workerName)
      throws IOException {
    try (Jedis jedis = getJedis()) {
      Pipeline p = jedis.pipelined();
      for (Digest blobDigest : blobDigests) {
        p.srem(casKey(blobDigest), workerName);
      }
      p.sync();
    } catch (JedisConnectionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(e);
    }
  }

  @Override
  public String getBlobLocation(Digest blobDigest) throws IOException {
    try (Jedis jedis = getJedis()) {
      return jedis.srandmember(casKey(blobDigest));
    } catch (JedisConnectionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(e);
    }
  }

  @Override
  public Set<String> getBlobLocationSet(Digest blobDigest) throws IOException {
    try (Jedis jedis = getJedis()) {
      return jedis.smembers(casKey(blobDigest));
    } catch (JedisConnectionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(e);
    }
  }

  @Override
  public Map<Digest, Set<String>> getBlobDigestsWorkers(Iterable<Digest> blobDigests)
      throws IOException {
    /* I WANT TO USE MGET */
    ImmutableMap.Builder<Digest, Set<String>> blobDigestsWorkers = new ImmutableMap.Builder<>();
    try (Jedis jedis = getJedis()) {
      for (Digest blobDigest : blobDigests) {
        Set<String> workers = jedis.smembers(casKey(blobDigest));
        if (workers.isEmpty()) {
          continue;
        }
        blobDigestsWorkers.put(blobDigest, workers);
      }
    } catch (JedisConnectionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(e);
    }
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
    String json;
    try (Jedis jedis = getJedis()) {
      json = getOperation(jedis, operationName);
    } catch (JedisConnectionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(e);
    }
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
    try (Jedis jedis = getJedis()) {
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
    } catch (JedisConnectionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(e);
    }
    return true;
  }

  private void queueOperation(Jedis jedis, String operationName) {
    if (jedis.hdel(config.getDispatchedOperationsHashName(), operationName) == 1) {
      System.err.println("RedisShardBackplane::queueOperation: WARNING Removed dispatched operation");
    }
    jedis.lpush(config.getQueuedOperationsListName(), operationName);
  }

  public Map<String, Operation> getOperationsMap() throws IOException {
    try (Jedis jedis = getJedis()) {
      ImmutableMap.Builder<String, Operation> builder = new ImmutableMap.Builder<>();
      for (Map.Entry<String, String> entry : jedis.hgetAll(config.getDispatchedOperationsHashName()).entrySet()) {
        builder.put(entry.getKey(), parseOperationJson(entry.getValue()));
      }
      return builder.build();
    } catch (JedisConnectionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(e);
    }
  }

  @Override
  public Iterable<String> getOperations() throws IOException {
    try (Jedis jedis = getJedis()) {
      throw new UnsupportedOperationException();
      /*
      Iterable<String> dispatchedOperations = jedis.hkeys(config.getDispatchedOperationsHashName());
      Iterable<String> queuedOperations = jedis.lrange(config.getQueuedOperationsListName(), 0, -1);
      return Iterables.concat(queuedOperations, dispatchedOperations, getCompletedOperations(jedis));
      */
    } catch (JedisConnectionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(e);
    }
  }

  @Override
  public ImmutableList<ShardDispatchedOperation> getDispatchedOperations() throws IOException {
    ImmutableList.Builder<ShardDispatchedOperation> builder = new ImmutableList.Builder<ShardDispatchedOperation>();
    try (Jedis jedis = getJedis()) {
      for (Map.Entry<String, String> entry : jedis.hgetAll(config.getDispatchedOperationsHashName()).entrySet()) {
        try {
          ShardDispatchedOperation.Builder dispatchedOperationBuilder = ShardDispatchedOperation.newBuilder();
          JsonFormat.parser().merge(entry.getValue(), dispatchedOperationBuilder);
          builder.add(dispatchedOperationBuilder.build());
        } catch (InvalidProtocolBufferException e) {
          System.err.println("RedisShardBackplane::getDispatchedOperations: removing invalid operation " + entry.getKey());
          e.printStackTrace();
          /* guess we don't want to spin on this */
          jedis.hdel(config.getDispatchedOperationsHashName(), entry.getKey());
        }
      }
    } catch (JedisConnectionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(e);
    }
    return builder.build();
  }

  @Override
  public String dispatchOperation() throws IOException, InterruptedException {
    String operationName = null;

    try (Jedis jedis = getJedis()) {
      List<String> result = null;
      while (result == null) {
        /* maybe we should really have a dispatch queue per registered worker */
        result = jedis.brpop(1, config.getQueuedOperationsListName());
        if (Thread.interrupted()) {
          throw new InterruptedException();
        }
      }
      if (result.size() == 2 && result.get(0).equals(config.getQueuedOperationsListName())) {
        ShardDispatchedOperation o = ShardDispatchedOperation.newBuilder()
            .setName(result.get(1))
            .setRequeueAt(System.currentTimeMillis() + 30 * 1000)
            .build();
        /* if the operation is already in the dispatch list, don't requeue */
        if (jedis.hsetnx(config.getDispatchedOperationsHashName(), o.getName(), JsonFormat.printer().print(o)) == 1) {
          operationName = result.get(1);
        }
      }
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
      operationName = null;
    } catch (JedisConnectionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(e);
    }

    return operationName;
  }

  @Override
  public boolean pollOperation(String operationName, Stage stage, long requeueAt) throws IOException {
    try (Jedis jedis = getJedis()) {
      boolean success = false;
      if (jedis.hexists(config.getDispatchedOperationsHashName(), operationName)) {
        ShardDispatchedOperation o = ShardDispatchedOperation.newBuilder()
            .setName(operationName)
            .setRequeueAt(requeueAt)
            .build();
        if (jedis.hset(config.getDispatchedOperationsHashName(), operationName, JsonFormat.printer().print(o)) == 0) {
          success = true;
        } else {
          /* someone else beat us to the punch, delete our incorrectly added key */
          jedis.hdel(config.getDispatchedOperationsHashName(), operationName);
        }
      }
      return success;
    } catch (InvalidProtocolBufferException e) {
      return false;
    } catch (JedisConnectionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(e);
    }
  }

  @Override
  public void requeueDispatchedOperation(Operation operation) throws IOException {
    try (Jedis jedis = getJedis()) {
      queueOperation(jedis, operation.getName());
    } catch (JedisConnectionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(e);
    }
  }

  private void completeOperation(Jedis jedis, String operationName) {
    if (jedis.hdel(config.getDispatchedOperationsHashName(), operationName) != 1) {
      System.err.println("RedisShardBackplane::completeOperation: WARNING " + operationName + " was not in dispatched list");
    }
  }

  @Override
  public void completeOperation(String operationName) throws IOException {
    try (Jedis jedis = getJedis()) {
      completeOperation(jedis, operationName);
    } catch (JedisConnectionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(e);
    }
  }

  @Override
  public void deleteOperation(String operationName) throws IOException {
    try (Jedis jedis = getJedis()) {
      Transaction t = jedis.multi();
      t.hdel(config.getDispatchedOperationsHashName(), operationName);
      t.lrem(config.getQueuedOperationsListName(), 0, operationName);
      t.del(operationKey(operationName));
      t.exec();

      operationSubscription.getSubscriber().onMessage(operationName, null);
    } catch (JedisConnectionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(e);
    }
  }

  private Jedis getJedis() {
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
    try (Jedis jedis = getJedis()) {
      jedis.setex(
          treeKey(inputRoot),
          config.getTreeExpire(),
          treeValue);
    } catch (JedisConnectionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(e);
    }
  }

  @Override
  public Iterable<Directory> getTree(Digest inputRoot) throws IOException {
    try (Jedis jedis = getJedis()) {
      String json = jedis.get(treeKey(inputRoot));
      if (json == null) {
        return null;
      }

      GetTreeResponse.Builder builder = GetTreeResponse.newBuilder();
      JsonFormat.parser().merge(json, builder);
      return builder.build().getDirectoriesList();
    } catch (JedisConnectionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(e);
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public void removeTree(Digest inputRoot) throws IOException {
    try (Jedis jedis = getJedis()) {
      jedis.del(treeKey(inputRoot));
    } catch (JedisConnectionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(e);
    }
  }
}
