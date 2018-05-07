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

import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.ShardBackplane;
import build.buildfarm.v1test.RedisShardBackplaneConfig;
import build.buildfarm.v1test.ShardDispatchedOperation;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.devtools.remoteexecution.v1test.ActionResult;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata;
import com.google.longrunning.Operation;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import javax.naming.ConfigurationException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

public class RedisShardBackplane implements ShardBackplane {
  private final RedisShardBackplaneConfig config;
  private final Map<String, List<Predicate<Operation>>> watchers;
  private final JedisPool pool;

  public RedisShardBackplane(RedisShardBackplaneConfig config) throws ConfigurationException {
    this.config = config;

    URI redisURI;
    try {
      redisURI = new URI(config.getRedisUri());
    } catch (URISyntaxException ex) {
      throw new ConfigurationException(ex.getMessage());
    }

    watchers = new ConcurrentHashMap<>();
    JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
    jedisPoolConfig.setMaxTotal(128);
    pool = new JedisPool(jedisPoolConfig, redisURI, /* connectionTimeout=*/ 30000, /* soTimeout=*/ 30000);

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

    new Thread(() -> {
      for(;;) {
        try (Jedis jedis = getJedis()) {
          jedis.subscribe(operationSubscriber, "OperationChannel");
        }
      }
    }).start();
  }

  @Override
  public boolean watchOperation(String operationName, Predicate<Operation> watcher) {
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
  public void addWorker(String workerName) {
    try (Jedis jedis = getJedis()) {
      jedis.sadd(config.getWorkersSetName(), workerName);
    }
  }

  @Override
  public void removeWorker(String workerName) {
    try (Jedis jedis = getJedis()) {
      jedis.srem(config.getWorkersSetName(), workerName);
    }
  }

  @Override
  public String getRandomWorker() {
    try (Jedis jedis = getJedis()) {
      return jedis.srandmember(config.getWorkersSetName());
    }
  }

  @Override
  public Set<String> getWorkerSet() {
    try (Jedis jedis = getJedis()) {
      return jedis.smembers(config.getWorkersSetName());
    }
  }

  @Override
  public boolean isWorker(String workerName) {
    try (Jedis jedis = getJedis()) {
      return jedis.sismember(config.getWorkersSetName(), workerName);
    }
  }

  @Override
  public ActionResult getActionResult(ActionKey actionKey) {
    try (Jedis jedis = getJedis()) {
      String json = jedis.hget(config.getActionCacheHashName(), DigestUtil.toString(actionKey.getDigest()));
      if (json == null) {
        return null;
      }

      try {
        ActionResult.Builder builder = ActionResult.newBuilder();
        JsonFormat.parser().merge(json, builder);
        return builder.build();
      } catch (InvalidProtocolBufferException ex) {
        ex.printStackTrace();
        return null;
      }
    }
  }

  @Override
  public void putActionResult(ActionKey actionKey, ActionResult actionResult) {
    try (Jedis jedis = getJedis()) {
      String json = JsonFormat.printer().print(actionResult);
      jedis.hset(config.getActionCacheHashName(), DigestUtil.toString(actionKey.getDigest()), json);
    } catch (InvalidProtocolBufferException ex) {
      ex.printStackTrace();
    }
  }

  @Override
  public void removeActionResult(ActionKey actionKey) {
    try (Jedis jedis = getJedis()) {
      jedis.hdel(config.getActionCacheHashName(), DigestUtil.toString(actionKey.getDigest()));
    }
  }

  @Override
  public void addBlobLocation(Digest blobDigest, String workerName) {
    try (Jedis jedis = getJedis()) {
      jedis.sadd(casKey(blobDigest), workerName);
    }
  }

  @Override
  public void removeBlobLocation(Digest blobDigest, String workerName) {
    try (Jedis jedis = getJedis()) {
      jedis.srem(casKey(blobDigest), workerName);
    }
  }

  @Override
  public String getBlobLocation(Digest blobDigest) {
    try (Jedis jedis = getJedis()) {
      return jedis.srandmember(casKey(blobDigest));
    }
  }

  @Override
  public Set<String> getBlobLocationSet(Digest blobDigest) {
    try (Jedis jedis = getJedis()) {
      return jedis.smembers(casKey(blobDigest));
    }
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
            .add(ExecuteOperationMetadata.getDescriptor())
            .build());
  }

  @Override
  public Operation getOperation(String operationName) {
    try (Jedis jedis = getJedis()) {
      String json = jedis.hget(config.getOperationsHashName(), operationName);
      if (json == null) {
        return null;
      }
      return parseOperationJson(json);
    }
  }

  @Override
  public boolean putOperation(Operation operation) {
    ExecuteOperationMetadata metadata = expectExecuteOperationMetadata(operation);

    boolean queue = metadata.getStage() == ExecuteOperationMetadata.Stage.QUEUED;
    boolean complete = !queue && operation.getDone();
    boolean publish = !queue && metadata.getStage() != ExecuteOperationMetadata.Stage.UNKNOWN;

    String json;
    try {
      JsonFormat.Printer operationPrinter = JsonFormat.printer().usingTypeRegistry(
          JsonFormat.TypeRegistry.newBuilder()
              .add(ExecuteOperationMetadata.getDescriptor())
              .build());
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
        jedis.publish(config.getOperationChannelName(), json);
      }
    }
    return true;
  }

  private static ExecuteOperationMetadata expectExecuteOperationMetadata(
      Operation operation) {
    Preconditions.checkState(
        operation.getMetadata().is(ExecuteOperationMetadata.class));
    try {
      return operation.getMetadata().unpack(ExecuteOperationMetadata.class);
    } catch(InvalidProtocolBufferException ex) {
      ex.printStackTrace();
      return null;
    }
  }

  private void queueOperation(Jedis jedis, String operationName) {
    jedis.lpush(config.getQueuedOperationsListName(), operationName);
  }

  @Override
  public String dispatchOperation() throws InterruptedException {
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
    }

    return operationName;
  }

  private void completeOperation(Jedis jedis, String operationName) {
    if (jedis.hdel(config.getDispatchedOperationsHashName(), operationName) == 1) {
      jedis.rpush(config.getCompletedOperationsListName(), operationName);
    }
  }

  private Jedis getJedis() {
    return pool.getResource();
  }

  private String casKey(Digest blobDigest) {
    return config.getCasPrefix() + ":" + DigestUtil.toString(blobDigest);
  }
}
