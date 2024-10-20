// Copyright 2022 The Buildfarm Authors. All rights reserved.
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
import build.buildfarm.common.redis.QueueDecorator;
import build.buildfarm.common.redis.RedisHashMap;
import build.buildfarm.common.redis.RedisHashtags;
import build.buildfarm.common.redis.RedisMap;
import build.buildfarm.common.redis.RedisNodeHashes;
import build.buildfarm.common.redis.RedisPriorityQueue;
import build.buildfarm.common.redis.RedisQueue;
import build.buildfarm.common.redis.RedisSetMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.SetMultimap;
import java.util.List;
import redis.clients.jedis.UnifiedJedis;

public class DistributedStateCreator {
  private static BuildfarmConfigs configs = BuildfarmConfigs.getInstance();

  public static DistributedState create(UnifiedJedis jedis) {
    DistributedState state = new DistributedState();

    // Create containers that make up the backplane
    state.actionCache = createActionCache();
    state.prequeue = createPrequeue(jedis);
    state.executionQueue = createExecutionQueue(jedis);
    state.blockedActions = new RedisMap(configs.getBackplane().getActionBlacklistPrefix());
    state.blockedInvocations = new RedisMap(configs.getBackplane().getInvocationBlacklistPrefix());
    state.toolInvocations =
        new RedisSetMap(
            configs.getBackplane().getToolInvocationsPrefix(),
            configs.getBackplane().getMaxToolInvocationTimeout(),
            /* expireOnEach= */ false);
    state.executions =
        new Executions(
            state.toolInvocations,
            configs.getBackplane().getOperationPrefix(), // FIXME change to Execution
            configs.getBackplane().getOperationExpire());
    state.processingExecutions = new RedisMap(configs.getBackplane().getProcessingPrefix());
    state.dispatchingExecutions = new RedisMap(configs.getBackplane().getDispatchingPrefix());
    state.dispatchedExecutions =
        new RedisHashMap(
            configs.getBackplane().getDispatchedOperationsHashName()); // FIXME change to Executions
    state.executeWorkers =
        new RedisHashMap(configs.getBackplane().getWorkersHashName() + "_execute");
    state.storageWorkers =
        new RedisHashMap(configs.getBackplane().getWorkersHashName() + "_storage");
    state.correlatedInvocationsIndex =
        new RedisSetMap(
            configs.getBackplane().getCorrelatedInvocationsIndexPrefix(),
            configs.getBackplane().getMaxCorrelatedInvocationsIndexTimeout(),
            /* expireOnEach= */ true);
    state.correlatedInvocations =
        new RedisSetMap(
            configs.getBackplane().getCorrelatedInvocationsPrefix(),
            configs.getBackplane().getMaxCorrelatedInvocationsTimeout(),
            /* expireOnEach= */ true);

    return state;
  }

  private static RedisMap createActionCache() {
    return new RedisMap(configs.getBackplane().getActionCachePrefix());
  }

  private static BalancedRedisQueue createPrequeue(UnifiedJedis jedis) {
    // Construct the prequeue so that elements are balanced across all redis nodes.
    return new BalancedRedisQueue(
        getPreQueuedOperationsListName(),
        getQueueHashes(jedis, getPreQueuedOperationsListName()),
        configs.getBackplane().getMaxPreQueueDepth(),
        getQueueDecorator());
  }

  private static ExecutionQueue createExecutionQueue(UnifiedJedis jedis) {
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
              getQueueDecorator(),
              getQueueHashes(jedis, getQueueName(queueConfig)),
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
              getQueueDecorator(),
              getQueueHashes(jedis, getQueuedOperationsListName()),
              defaultProvisions);
      provisionedQueues.add(defaultQueue);
    }

    return new ExecutionQueue(provisionedQueues.build(), configs.getBackplane().getMaxQueueDepth());
  }

  static List<String> getQueueHashes(UnifiedJedis jedis, String queueName) {
    return RedisNodeHashes.getEvenlyDistributedHashesWithPrefix(
        jedis, RedisHashtags.existingHash(queueName));
  }

  private static SetMultimap<String, String> toMultimap(List<Platform.Property> provisions) {
    SetMultimap<String, String> set = LinkedHashMultimap.create();
    for (Platform.Property property : provisions) {
      set.put(property.getName(), property.getValue());
    }
    return set;
  }

  private static QueueDecorator getQueueDecorator() {
    return configs.getBackplane().isPriorityQueue()
        ? RedisPriorityQueue::decorate
        : RedisQueue::decorate;
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
