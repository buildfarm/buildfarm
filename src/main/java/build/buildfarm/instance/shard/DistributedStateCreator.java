// Copyright 2022 The Bazel Authors. All rights reserved.
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

import build.bazel.remote.execution.v2.Platform;
import build.buildfarm.common.redis.BalancedRedisQueue;
import build.buildfarm.common.redis.ProvisionedRedisQueue;
import build.buildfarm.common.redis.RedisClient;
import build.buildfarm.common.redis.RedisHashMap;
import build.buildfarm.common.redis.RedisHashtags;
import build.buildfarm.common.redis.RedisMap;
import build.buildfarm.common.redis.RedisNodeHashes;
import build.buildfarm.v1test.ProvisionedQueue;
import build.buildfarm.v1test.QueueType;
import build.buildfarm.v1test.RedisShardBackplaneConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.SetMultimap;
import java.io.IOException;
import java.util.List;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

public class DistributedStateCreator {
  public static DistributedState create(RedisClient client, RedisShardBackplaneConfig config)
      throws IOException {
    DistributedState state = new DistributedState();

    // Create containers that make up the backplane
    state.casWorkerMap = createCasWorkerMap(config);
    state.actionCache = createActionCache(config);
    state.prequeue = createPrequeue(client, config);
    state.operationQueue = createOperationQueue(client, config);
    state.blockedActions = new RedisMap(config.getActionBlacklistPrefix());
    state.blockedInvocations = new RedisMap(config.getInvocationBlacklistPrefix());
    state.processingOperations = new RedisMap(config.getProcessingPrefix());
    state.dispatchingOperations = new RedisMap(config.getDispatchingPrefix());
    state.dispatchedOperations = new RedisHashMap(config.getDispatchedOperationsHashName());
    state.workers = new RedisHashMap(config.getWorkersHashName());

    return state;
  }

  private static CasWorkerMap createCasWorkerMap(RedisShardBackplaneConfig config) {
    if (config.getCacheCas()) {
      RedissonClient redissonClient = createRedissonClient();
      return new RedissonCasWorkerMap(redissonClient, config.getCasPrefix(), config.getCasExpire());
    } else {
      return new JedisCasWorkerMap(config.getCasPrefix(), config.getCasExpire());
    }
  }

  private static RedisMap createActionCache(RedisShardBackplaneConfig config) {
    return new RedisMap(config.getActionCachePrefix());
  }

  private static RedissonClient createRedissonClient() {
    Config redissonConfig = new Config();
    return Redisson.create(redissonConfig);
  }

  private static BalancedRedisQueue createPrequeue(
      RedisClient client, RedisShardBackplaneConfig config) throws IOException {
    // Construct the prequeue so that elements are balanced across all redis nodes.
    return new BalancedRedisQueue(
        getPreQueuedOperationsListName(config),
        getQueueHashes(client, getPreQueuedOperationsListName(config)),
        config.getMaxPreQueueDepth(),
        queueTypeToSr(config));
  }

  private static OperationQueue createOperationQueue(
      RedisClient client, RedisShardBackplaneConfig config) throws IOException {
    // Construct an operation queue based on configuration.
    // An operation queue consists of multiple provisioned queues in which the order dictates the
    // eligibility and placement of operations.
    // Therefore, it is recommended to have a final provision queue with no actual platform
    // requirements.  This will ensure that all operations are eligible for the final queue.
    ImmutableList.Builder<ProvisionedRedisQueue> provisionedQueues = new ImmutableList.Builder<>();
    for (ProvisionedQueue queueConfig : config.getProvisionedQueues().getQueuesList()) {
      ProvisionedRedisQueue provisionedQueue =
          new ProvisionedRedisQueue(
              getQueueName(queueConfig, config),
              queueTypeToSr(config),
              getQueueHashes(client, getQueueName(queueConfig, config)),
              toMultimap(queueConfig.getPlatform().getPropertiesList()),
              queueConfig.getAllowUnmatched());
      provisionedQueues.add(provisionedQueue);
    }
    // If there is no configuration for provisioned queues, we might consider that an error.
    // After all, the operation queue is made up of n provisioned queues, and if there were no
    // provisioned queues provided, we can not properly construct the operation queue.
    // In this case however, we will automatically provide a default queue will full eligibility on
    // all operations.
    // This will ensure the expected behavior for the paradigm in which all work is put on the same
    // queue.
    if (config.getProvisionedQueues().getQueuesList().isEmpty()) {
      SetMultimap defaultProvisions = LinkedHashMultimap.create();
      defaultProvisions.put(
          ProvisionedRedisQueue.WILDCARD_VALUE, ProvisionedRedisQueue.WILDCARD_VALUE);
      ProvisionedRedisQueue defaultQueue =
          new ProvisionedRedisQueue(
              getQueuedOperationsListName(config),
              queueTypeToSr(config),
              getQueueHashes(client, getQueuedOperationsListName(config)),
              defaultProvisions);
      provisionedQueues.add(defaultQueue);
    }

    return new OperationQueue(provisionedQueues.build(), config.getMaxQueueDepth());
  }

  static List<String> getQueueHashes(RedisClient client, String queueName) throws IOException {
    return client.call(
        jedis ->
            RedisNodeHashes.getEvenlyDistributedHashesWithPrefix(
                jedis, RedisHashtags.existingHash(queueName)));
  }

  private static SetMultimap<String, String> toMultimap(List<Platform.Property> provisions) {
    SetMultimap<String, String> set = LinkedHashMultimap.create();
    for (Platform.Property property : provisions) {
      set.put(property.getName(), property.getValue());
    }
    return set;
  }

  private static String queueTypeToSr(RedisShardBackplaneConfig config) {
    QueueType queue = config.getRedisQueueType();
    return queue.toString().toLowerCase();
  }

  private static String getQueuedOperationsListName(RedisShardBackplaneConfig config) {
    String name = config.getQueuedOperationsListName();
    String queue_type = queueTypeToSr(config);
    return createFullQueueName(name, queue_type);
  }

  private static String getPreQueuedOperationsListName(RedisShardBackplaneConfig config) {
    String name = config.getPreQueuedOperationsListName();
    String queue_type = queueTypeToSr(config);
    return createFullQueueName(name, queue_type);
  }

  private static String getQueueName(ProvisionedQueue pconfig, RedisShardBackplaneConfig rconfig) {
    String name = pconfig.getName();
    String queue_type = queueTypeToSr(rconfig);
    return createFullQueueName(name, queue_type);
  }

  private static String createFullQueueName(String base, String type) {
    // To maintain forwards compatibility, we do not append the type to the regular queue
    // implementation.
    return ((!type.equals("regular")) ? base + "_" + type : base);
  }
}
