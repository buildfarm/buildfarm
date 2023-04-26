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
import build.buildfarm.common.config.BuildfarmConfigs;
import build.buildfarm.common.config.Queue;
import build.buildfarm.common.redis.BalancedRedisQueue;
import build.buildfarm.common.redis.ProvisionedRedisQueue;
import build.buildfarm.common.redis.RedisClient;
import build.buildfarm.common.redis.RedisHashMap;
import build.buildfarm.common.redis.RedisHashtags;
import build.buildfarm.common.redis.RedisMap;
import build.buildfarm.common.redis.RedisNodeHashes;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.SetMultimap;
import java.io.IOException;
import java.util.List;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

public class DistributedStateCreator {
  private static BuildfarmConfigs configs = BuildfarmConfigs.getInstance();

  public static DistributedState create(RedisClient client) throws IOException {
    DistributedState state = new DistributedState();

    // Create containers that make up the backplane
    state.casWorkerMap = createCasWorkerMap();
    state.actionCache = createActionCache();
    state.prequeue = createPrequeue(client);
    state.operationQueue = createOperationQueue(client);
    state.blockedActions = new RedisMap(configs.getBackplane().getActionBlacklistPrefix());
    state.blockedInvocations = new RedisMap(configs.getBackplane().getInvocationBlacklistPrefix());
    state.operations =
        new Operations(
            configs.getBackplane().getOperationPrefix(),
            configs.getBackplane().getOperationExpire());
    state.processingOperations = new RedisMap(configs.getBackplane().getProcessingPrefix());
    state.dispatchingOperations = new RedisMap(configs.getBackplane().getDispatchingPrefix());
    state.dispatchedOperations =
        new RedisHashMap(configs.getBackplane().getDispatchedOperationsHashName());
    state.executeWorkers =
        new RedisHashMap(configs.getBackplane().getWorkersHashName() + "_execute");
    state.storageWorkers =
        new RedisHashMap(configs.getBackplane().getWorkersHashName() + "_storage");

    return state;
  }

  private static CasWorkerMap createCasWorkerMap() {
    if (configs.getBackplane().isCacheCas()) {
      RedissonClient redissonClient = createRedissonClient();
      return new RedissonCasWorkerMap(
          redissonClient,
          configs.getBackplane().getCasPrefix(),
          configs.getBackplane().getCasExpire());
    } else {
      return new JedisCasWorkerMap(
          configs.getBackplane().getCasPrefix(), configs.getBackplane().getCasExpire());
    }
  }

  private static RedisMap createActionCache() {
    return new RedisMap(configs.getBackplane().getActionCachePrefix());
  }

  private static RedissonClient createRedissonClient() {
    Config redissonConfig = new Config();
    return Redisson.create(redissonConfig);
  }

  private static BalancedRedisQueue createPrequeue(RedisClient client) throws IOException {
    // Construct the prequeue so that elements are balanced across all redis nodes.
    return new BalancedRedisQueue(
        getPreQueuedOperationsListName(),
        getQueueHashes(client, getPreQueuedOperationsListName()),
        configs.getBackplane().getMaxPreQueueDepth(),
        getQueueType());
  }

  private static OperationQueue createOperationQueue(RedisClient client) throws IOException {
    // Construct an operation queue based on configuration.
    // An operation queue consists of multiple provisioned queues in which the order dictates the
    // eligibility and placement of operations.
    // Therefore, it is recommended to have a final provision queue with no actual platform
    // requirements.  This will ensure that all operations are eligible for the final queue.
    ImmutableList.Builder<ProvisionedRedisQueue> provisionedQueues = new ImmutableList.Builder<>();
    for (Queue queueConfig : configs.getBackplane().getQueues()) {
      ProvisionedRedisQueue provisionedQueue =
          new ProvisionedRedisQueue(
              getQueueName(queueConfig),
              getQueueType(),
              getQueueHashes(client, getQueueName(queueConfig)),
              toMultimap(queueConfig.getPlatform().getPropertiesList()),
              queueConfig.isAllowUnmatched());
      provisionedQueues.add(provisionedQueue);
    }
    // If there is no configuration for provisioned queues, we might consider that an error.
    // After all, the operation queue is made up of n provisioned queues, and if there were no
    // provisioned queues provided, we can not properly construct the operation queue.
    // In this case however, we will automatically provide a default queue will full eligibility on
    // all operations.
    // This will ensure the expected behavior for the paradigm in which all work is put on the same
    // queue.
    if (configs.getBackplane().getQueues().length == 0) {
      SetMultimap defaultProvisions = LinkedHashMultimap.create();
      defaultProvisions.put(
          ProvisionedRedisQueue.WILDCARD_VALUE, ProvisionedRedisQueue.WILDCARD_VALUE);
      ProvisionedRedisQueue defaultQueue =
          new ProvisionedRedisQueue(
              getQueuedOperationsListName(),
              getQueueType(),
              getQueueHashes(client, getQueuedOperationsListName()),
              defaultProvisions);
      provisionedQueues.add(defaultQueue);
    }

    return new OperationQueue(provisionedQueues.build(), configs.getBackplane().getMaxQueueDepth());
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

  private static Queue.QUEUE_TYPE getQueueType() {
    return configs.getBackplane().isPriorityQueue()
        ? Queue.QUEUE_TYPE.priority
        : Queue.QUEUE_TYPE.standard;
  }

  private static String getQueuedOperationsListName() {
    String name = configs.getBackplane().getQueuedOperationsListName();
    return createFullQueueName(name, getQueueType());
  }

  private static String getPreQueuedOperationsListName() {
    String name = configs.getBackplane().getPreQueuedOperationsListName();
    return createFullQueueName(name, getQueueType());
  }

  private static String getQueueName(Queue pconfig) {
    String name = pconfig.getName();
    return createFullQueueName(name, getQueueType());
  }

  private static String createFullQueueName(String base, Queue.QUEUE_TYPE type) {
    // To maintain forwards compatibility, we do not append the type to the regular queue
    // implementation.
    return ((!type.equals(Queue.QUEUE_TYPE.standard)) ? base + "_" + type : base);
  }
}
