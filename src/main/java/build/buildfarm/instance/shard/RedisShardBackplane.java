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
import java.util.function.Function;
import java.util.function.Predicate;
import javax.naming.ConfigurationException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class RedisShardBackplane implements ShardBackplane {
  private final RedisShardBackplaneConfig config;
  private final Map<String, List<Predicate<Operation>>> watchers = new ConcurrentHashMap<>();
  private final JedisPool pool;
  private final Thread subscriptionThread;
  private final Function<Operation, Operation> onPublish;
  private final Function<Operation, Operation> onComplete;

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
      Function<Operation, Operation> onComplete) throws ConfigurationException {
    this.config = config;
    this.onPublish = onPublish;
    this.onComplete = onComplete;

    URI redisURI;
    try {
      redisURI = new URI(config.getRedisUri());
    } catch (URISyntaxException ex) {
      throw new ConfigurationException(ex.getMessage());
    }

    JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
    jedisPoolConfig.setMaxTotal(128);
    pool = new JedisPool(jedisPoolConfig, redisURI, /* connectionTimeout=*/ 30000, /* soTimeout=*/ 30000);

    if (config.getSubscribeToOperation()) {
      JedisPubSub operationSubscriber = new JedisPubSub() {
        boolean subscribed = false;

        @Override
        public void onMessage(String channel, String message) {
          Operation operation = parseOperationJson(message);
          synchronized (watchers) {
            List<Predicate<Operation>> operationWatchers =
                watchers.get(operation.getName());
            if (operationWatchers == null)
              return;
            if (operation.getDone()) {
              watchers.remove(operation.getName());
            }
            long unfilteredWatcherCount = operationWatchers.size();
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
          }
        }
      };

      subscriptionThread = new Thread(() -> {
        for(;;) {
          try (Jedis jedis = getJedis()) {
            jedis.subscribe(operationSubscriber, config.getOperationChannelName());
          }
        }
      });
    } else {
      subscriptionThread = null;
    }
  }

  @Override
  public void start() {
    if (subscriptionThread != null) {
      subscriptionThread.start();
    }
  }

  @Override
  public void stop() {
    if (subscriptionThread != null) {
      subscriptionThread.stop();
      // subscriptionThread.join();
    }
    pool.close();
  }

  @Override
  public boolean watchOperation(String operationName, Predicate<Operation> watcher) throws IOException {
    Operation completedOperation = null;
    synchronized(watchers) {
      List<Predicate<Operation>> operationWatchers = watchers.get(operationName);
      if (operationWatchers == null) {
        /* we can race on the synchronization and miss a done, where the
         * watchers list has been removed, making it necessary to check for the
         * operation within this context */
        Operation operation = getOperation(operationName);
        if (operation == null) {
          return false;
        } else if (operation.getDone()) {
          completedOperation = operation;
        } else {
          operationWatchers = new ArrayList<Predicate<Operation>>();
          watchers.put(operationName, operationWatchers);
        }
      }
      // could still be null with a done
      if (operationWatchers != null) {
        operationWatchers.add(watcher);
      }
    }
    if (completedOperation != null) {
      return watcher.test(completedOperation);
    }

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
      String json = jedis.hget(
          config.getActionCacheHashName(),
          DigestUtil.toString(actionKey.getDigest()));
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
      jedis.hset(
          config.getActionCacheHashName(),
          DigestUtil.toString(actionKey.getDigest()),
          json);
    } catch (JedisConnectionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(e);
    } catch (InvalidProtocolBufferException ex) {
      ex.printStackTrace();
    }
  }

  private void removeActionResult(Jedis jedis, ActionKey actionKey) {
    jedis.hdel(config.getActionCacheHashName(), DigestUtil.toString(actionKey.getDigest()));
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
        p.hdel(
            config.getActionCacheHashName(),
            DigestUtil.toString(actionKey.getDigest()));
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
    ScanParams scanParams = new ScanParams().count(count);
    final ScanResult<Map.Entry<String, String>> scanResult;
    try (Jedis jedis = getJedis()) {
      scanResult = jedis.hscan(config.getActionCacheHashName(), scanToken, scanParams);
    } catch (JedisConnectionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(e);
    }
    return new ActionCacheScanResult(
        scanResult.getStringCursor().equals(SCAN_POINTER_START) ? null : scanResult.getStringCursor(),
        Iterables.transform(
            scanResult.getResult(),
            (entry) -> new AbstractMap.SimpleEntry<ActionKey, ActionResult>(
                DigestUtil.asActionKey(DigestUtil.parseDigest(entry.getKey())),
                parseActionResult(entry.getValue()))));
  }

  @Override
  public void addBlobLocation(Digest blobDigest, String workerName) throws IOException {
    try (Jedis jedis = getJedis()) {
      jedis.sadd(casKey(blobDigest), workerName);
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
        p.sadd(casKey(blobDigest), workerName);
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

  private static Operation parseOperationJson(String operationJson) {
    try {
      Operation.Builder operationBuilder = Operation.newBuilder();
      getOperationParser().merge(operationJson, operationBuilder);
      return operationBuilder.build();
    } catch (InvalidProtocolBufferException ex) {
      ex.printStackTrace();
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

  private Operation getOperation(Jedis jedis, String operationName) {
    String json = jedis.hget(config.getOperationsHashName(), operationName);
    if (json == null) {
      return null;
    }
    return parseOperationJson(json);
  }

  @Override
  public Operation getOperation(String operationName) throws IOException {
    try (Jedis jedis = getJedis()) {
      return getOperation(jedis, operationName);
    } catch (JedisConnectionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(e);
    }
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
    } catch (InvalidProtocolBufferException ex) {
      ex.printStackTrace();
      return false;
    }

    try (Jedis jedis = getJedis()) {
      if (complete) {
        completeOperation(jedis, operation.getName());
      }
      jedis.hset(config.getOperationsHashName(), operation.getName(), json);
      if (queue) {
        queueOperation(jedis, operation.getName());
      }
      if (publish) {
        jedis.publish(
            config.getOperationChannelName(),
            operationPrinter.print(onPublish.apply(operation)));
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
    jedis.hdel(config.getDispatchedOperationsHashName(), operationName);
    jedis.lpush(config.getQueuedOperationsListName(), operationName);
  }

  private Iterable<String> getCompletedOperations(Jedis jedis) {
    return jedis.lrange(config.getCompletedOperationsListName(), 0, -1);
  }

  public Iterable<String> getCompletedOperations() throws IOException {
    try (Jedis jedis = getJedis()) {
      return jedis.lrange(config.getCompletedOperationsListName(), 0, -1);
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
      Iterable<String> dispatchedOperations = jedis.hkeys(config.getDispatchedOperationsHashName());
      Iterable<String> queuedOperations = jedis.lrange(config.getQueuedOperationsListName(), 0, -1);
      return Iterables.concat(queuedOperations, dispatchedOperations, getCompletedOperations(jedis));
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
      for (Map.Entry<String, String> e : jedis.hgetAll(config.getDispatchedOperationsHashName()).entrySet()) {
        try {
          ShardDispatchedOperation.Builder dispatchedOperationBuilder = ShardDispatchedOperation.newBuilder();
          JsonFormat.parser().merge(e.getValue(), dispatchedOperationBuilder);
          builder.add(dispatchedOperationBuilder.build());
        } catch (InvalidProtocolBufferException ex) {
          /* guess we don't want to spin on this */
          jedis.hdel(config.getDispatchedOperationsHashName(), e.getKey());
          ex.printStackTrace();
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
  public String dispatchOperation() throws InterruptedException, IOException {
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
    } catch (InvalidProtocolBufferException ex) {
      ex.printStackTrace();
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
    } catch (InvalidProtocolBufferException ex) {
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
    if (jedis.hdel(config.getDispatchedOperationsHashName(), operationName) == 1) {
      jedis.rpush(config.getCompletedOperationsListName(), operationName);
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

  public void deleteAllCompletedOperation(Iterable<String> operationNames) throws IOException {
    try (Jedis jedis = getJedis()) {
      Transaction t = jedis.multi();
      for (String operationName : operationNames) {
        t.lrem(config.getCompletedOperationsListName(), 0, operationName);
        t.hdel(config.getOperationsHashName(), operationName);
      }
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
  public void deleteOperation(String operationName) throws IOException {
    try (Jedis jedis = getJedis()) {
      /* FIXME moar transaction */
      jedis.hdel(config.getDispatchedOperationsHashName(), operationName);
      jedis.lrem(config.getQueuedOperationsListName(), 0, operationName);
      jedis.lrem(config.getCompletedOperationsListName(), 0, operationName);
      jedis.hdel(config.getOperationsHashName(), operationName);

      // operationSubscriber.onMessage(config.getOperationChannelName(), operationJson);
    } catch (JedisConnectionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(e);
    }
  }

  @Override
  public long getCompletedOperationsCount() throws IOException {
    try (Jedis jedis = getJedis()) {
      return jedis.llen(config.getCompletedOperationsListName());
    } catch (JedisConnectionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(e);
    }
  }

  @Override
  public String popOldestCompletedOperation() throws IOException {
    try (Jedis jedis = getJedis()) {
      return jedis.lpop(config.getCompletedOperationsListName());
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

  @Override
  public void putTree(Digest inputRoot, Iterable<Directory> directories) throws IOException {
    try (Jedis jedis = getJedis()) {
      jedis.set(treeKey(inputRoot), JsonFormat.printer().print(GetTreeResponse.newBuilder()
          .addAllDirectories(directories)
          .build()));
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
