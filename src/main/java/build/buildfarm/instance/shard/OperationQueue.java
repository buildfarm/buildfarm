// Copyright 2020 The Bazel Authors. All rights reserved.
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
import build.buildfarm.common.BuildfarmExecutors;
import build.buildfarm.common.StringVisitor;
import build.buildfarm.common.redis.BalancedRedisQueue;
import build.buildfarm.common.redis.ProvisionedRedisQueue;
import build.buildfarm.v1test.OperationQueueStatus;
import build.buildfarm.v1test.QueueStatus;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.SetMultimap;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import redis.clients.jedis.JedisCluster;

/**
 * @class OperationQueue
 * @brief The operation queue of the shard backplane.
 * @details The operation queue can be split into multiple queues according to platform execution
 *     information.
 */
public class OperationQueue {
  /**
   * @field maxQueueSize
   * @brief The maximum amount of elements that should be added to the queue.
   * @details This is used to avoid placing too many elements onto the queue at any given time. For
   *     infinitely sized queues, use -1.
   */
  private final int maxQueueSize;

  // threads that each target a specific queue and block on dequeuing.
  ExecutorService dequeueService;

  // This is used for dequeueing threads to put operations and for workers to wait on.
  BlockingQueue<String> dequeued = new ArrayBlockingQueue<>(1);

  /**
   * @field queues
   * @brief Different queues based on platform execution requirements.
   * @details The appropriate queues are chosen based on given properties.
   */
  private final List<ProvisionedRedisQueue> queues;

  /**
   * @field currentDequeueIndex
   * @brief The current queue index to dequeue from.
   * @details Used in a round-robin fashion to ensure an even distribution of dequeues across
   *     matched queues.
   */
  private int currentDequeueIndex = 0;

  /**
   * @brief Constructor.
   * @details Construct the operation queue with various provisioned redis queues.
   * @param queues Provisioned queues.
   */
  public OperationQueue(List<ProvisionedRedisQueue> queues) {
    this.queues = queues;
    this.maxQueueSize = -1; // infinite size
  }

  /**
   * @brief Constructor.
   * @details Construct the operation queue with various provisioned redis queues.
   * @param queues Provisioned queues.
   * @param maxQueueSize The maximum amount of elements that should be added to the queue.
   */
  public OperationQueue(List<ProvisionedRedisQueue> queues, int maxQueueSize) {
    this.queues = queues;
    this.maxQueueSize = maxQueueSize;
  }

  public void startDequeuePool(JedisCluster jedis) {
    List<BalancedRedisQueue> queues = chooseAllQueues();
    dequeueService = BuildfarmExecutors.getOperationDequeuePool(queues.size());

    // Spawn a thread to block on each queue.
    IntStream.range(0, queues.size())
        .forEach(
            index ->
                dequeueService.execute(
                    () -> {
                      while (true) {
                        try {
                          String value = queues.get(index).dequeue(jedis);
                          if (value != null) {
                            dequeued.put(value);
                          }
                        } catch (Exception e) {
                        }
                      }
                    }));
  }

  /**
   * @brief Visit each element in the dequeue.
   * @details Enacts a visitor over each element in the dequeue.
   * @param jedis Jedis cluster client.
   * @param visitor A visitor for each visited element in the queue.
   */
  public void visitDequeue(JedisCluster jedis, StringVisitor visitor) {
    for (ProvisionedRedisQueue provisionedQueue : queues) {
      provisionedQueue.queue().visitDequeue(jedis, visitor);
    }
  }

  /**
   * @brief Remove element from dequeue.
   * @details Removes an element from the dequeue and specifies whether it was removed.
   * @param jedis Jedis cluster client.
   * @param val The value to remove.
   * @return Whether or not the value was removed.
   * @note Suggested return identifier: wasRemoved.
   */
  public boolean removeFromDequeue(JedisCluster jedis, String val) {
    for (ProvisionedRedisQueue provisionedQueue : queues) {
      if (provisionedQueue.queue().removeFromDequeue(jedis, val)) {
        return true;
      }
    }
    return false;
  }

  /**
   * @brief Visit each element in the queue.
   * @details Enacts a visitor over each element in the queue.
   * @param jedis Jedis cluster client.
   * @param visitor A visitor for each visited element in the queue.
   */
  public void visit(JedisCluster jedis, StringVisitor visitor) {
    for (ProvisionedRedisQueue provisionedQueue : queues) {
      provisionedQueue.queue().visit(jedis, visitor);
    }
  }

  /**
   * @brief Get size.
   * @details Checks the current length of the queue.
   * @param jedis Jedis cluster client.
   * @return The current length of the queue.
   * @note Suggested return identifier: length.
   */
  public long size(JedisCluster jedis) {
    // the accumulated size of all of the queues
    return queues.stream().mapToInt(i -> (int) i.queue().size(jedis)).sum();
  }

  /**
   * @brief Get dequeue name.
   * @details Get the name of the internal dequeue used by the queue. since each internal queue has
   *     their own dequeue, this name is generic without the hashtag.
   * @return The name of the queue.
   * @note Overloaded.
   * @note Suggested return identifier: name.
   */
  @SuppressWarnings("SameReturnValue")
  public String getDequeueName() {
    return "operation_dequeue";
  }

  /**
   * @brief Get dequeue name.
   * @details Get the name of the internal dequeue used by the queue. since each internal queue has
   *     their own dequeue, this name is generic without the hashtag.
   * @param provisions Provisions used to select an eligible queue.
   * @return The name of the queue.
   * @note Overloaded.
   * @note Suggested return identifier: name.
   */
  public String getDequeueName(List<Platform.Property> provisions) {
    BalancedRedisQueue queue = chooseEligibleQueue(provisions);
    return queue.getDequeueName();
  }

  /**
   * @brief Get internal queue name.
   * @details Get the name of the internal queue based on the platform properties.
   * @param provisions Provisions used to select an eligible queue.
   * @return The name of the queue.
   * @note Suggested return identifier: name.
   */
  public String getName(List<Platform.Property> provisions) {
    BalancedRedisQueue queue = chooseEligibleQueue(provisions);
    return queue.getName();
  }

  /**
   * @brief Push a value onto the queue.
   * @details Adds the value into one of the internal backend redis queues.
   * @param jedis Jedis cluster client.
   * @param provisions Provisions used to select an eligible queue.
   * @param val The value to push onto the queue.
   */
  public void push(
      JedisCluster jedis, List<Platform.Property> provisions, String val, int priority) {
    BalancedRedisQueue queue = chooseEligibleQueue(provisions);
    queue.push(jedis, val, (double) priority);
  }

  /**
   * @brief Pop element into internal dequeue and return value.
   * @details This pops the element from one queue atomically into an internal list called the
   *     dequeue. It will perform an exponential backoff. Null is returned if the overall backoff
   *     times out.
   * @param jedis Jedis cluster client.
   * @param provisions Provisions used to select an eligible queue.
   * @return The value of the transfered element. null if the thread was interrupted.
   * @note Suggested return identifier: val.
   */
  public String dequeue(JedisCluster jedis, List<Platform.Property> provisions)
      throws InterruptedException {
    // Select all matched queues, and attempt dequeuing via round-robin.
    List<BalancedRedisQueue> queues = chooseEligibleQueues(provisions);
    int index = roundRobinPopIndex(queues);
    String value = queues.get(index).nonBlockingDequeue(jedis);

    // Keep iterating over matched queues until we find one that is non-empty and provides a
    // dequeued value.
    while (value == null) {
      index = roundRobinPopIndex(queues);
      value = queues.get(index).nonBlockingDequeue(jedis);
    }
    return value;
  }

  /**
   * @brief Get status information about the queue.
   * @details Helpful for understanding the current load on the queue and how elements are balanced.
   * @param jedis Jedis cluster client.
   * @return The current status of the queue.
   * @note Overloaded.
   * @note Suggested return identifier: status.
   */
  public OperationQueueStatus status(JedisCluster jedis) {
    // get properties
    List<QueueStatus> provisions = new ArrayList<>();
    for (ProvisionedRedisQueue provisionedQueue : queues) {
      provisions.add(provisionedQueue.queue().status(jedis));
    }

    // build proto
    return OperationQueueStatus.newBuilder()
        .setSize(size(jedis))
        .addAllProvisions(provisions)
        .build();
  }

  /**
   * @brief Get status information about the queue.
   * @details Helpful for understanding the current load on the queue and how elements are balanced.
   * @param jedis Jedis cluster client.
   * @param provisions Provisions used to select an eligible queue.
   * @return The current status of the queue.
   * @note Overloaded.
   * @note Suggested return identifier: status.
   */
  public QueueStatus status(JedisCluster jedis, List<Platform.Property> provisions) {
    BalancedRedisQueue queue = chooseEligibleQueue(provisions);
    return queue.status(jedis);
  }

  /**
   * @brief Checks required properties for eligibility.
   * @details Checks whether the properties given fulfill all of the required provisions for the
   *     operation queue to accept it.
   * @param properties Properties to check that requirements are met.
   * @return Whether the operation queue will accept an operation containing the given properties.
   * @note Suggested return identifier: isEligible.
   */
  public boolean isEligible(List<Platform.Property> properties) {
    for (ProvisionedRedisQueue provisionedQueue : queues) {
      if (provisionedQueue.isEligible(toMultimap(properties))) {
        return true;
      }
    }
    return false;
  }

  /**
   * @brief Whether or not more elements can be added to the queue based on the queue's configured
   *     max size.
   * @details Compares the size of the queue to configured max size. Queues may be configured to be
   *     infinite in size.
   * @param jedis Jedis cluster client.
   * @return Whether are not a new element can be added to the queue based on its current size.
   */
  public boolean canQueue(JedisCluster jedis) {
    return maxQueueSize < 0 || size(jedis) < maxQueueSize;
  }

  /**
   * @brief Choose an eligible queue based on given properties.
   * @details We use the platform execution properties of a queue entry to determine the appropriate
   *     queue. If there no eligible queues, an exception is thrown.
   * @param provisions Provisions to check that requirements are met.
   * @return The chosen queue.
   * @note Suggested return identifier: queue.
   */
  private BalancedRedisQueue chooseEligibleQueue(List<Platform.Property> provisions) {
    for (ProvisionedRedisQueue provisionedQueue : queues) {
      if (provisionedQueue.isEligible(toMultimap(provisions))) {
        return provisionedQueue.queue();
      }
    }

    throwNoEligibleQueueException(provisions);
    return null;
  }

  private List<BalancedRedisQueue> chooseAllQueues() {
    return queues.stream()
        .map(provisionedQueue -> provisionedQueue.queue())
        .collect(Collectors.toList());
  }

  /**
   * @brief Choose an eligible queues based on given properties.
   * @details We use the platform execution properties of a queue entry to determine the appropriate
   *     queues. If there no eligible queues, an exception is thrown.
   * @param provisions Provisions to check that requirements are met.
   * @return The chosen queues.
   * @note Suggested return identifier: queues.
   */
  private List<BalancedRedisQueue> chooseEligibleQueues(List<Platform.Property> provisions) {
    List<BalancedRedisQueue> eligibleQueues =
        queues.stream()
            .filter(provisionedQueue -> provisionedQueue.isEligible(toMultimap(provisions)))
            .map(provisionedQueue -> provisionedQueue.queue())
            .collect(Collectors.toList());

    if (eligibleQueues.isEmpty()) {
      throwNoEligibleQueueException(provisions);
    }

    return eligibleQueues;
  }

  /**
   * @brief Throw an exception that explains why there are no eligible queues.
   * @details This function should only be called, when there were no matched queues.
   * @param provisions Provisions to check that requirements are met.
   * @return no return.
   */
  private void throwNoEligibleQueueException(List<Platform.Property> provisions) {
    // At this point, we were unable to match an action to an eligible queue.
    // We will build an error explaining why the matching failed. This will help user's properly
    // configure their queue or adjust the execution_properties of their actions.
    StringBuilder eligibilityResults =
        new StringBuilder("Below are the eligibility results for each provisioned queue:\n");
    for (ProvisionedRedisQueue provisionedQueue : queues) {
      eligibilityResults.append(provisionedQueue.explainEligibility(toMultimap(provisions)));
    }

    throw new RuntimeException(
        "there are no eligible queues for the provided execution requirements."
            + " One solution to is to configure a provision queue with no requirements which would be eligible to all operations."
            + " See https://github.com/bazelbuild/bazel-buildfarm/wiki/Shard-Platform-Operation-Queue for details. "
            + eligibilityResults);
  }

  /**
   * @brief Get the current queue index for round-robin dequeues.
   * @details Adjusts the round-robin index for next call.
   * @param matchedQueues The queues to round robin.
   * @return The current round-robin index.
   * @note Suggested return identifier: queueIndex.
   */
  private int roundRobinPopIndex(List<BalancedRedisQueue> matchedQueues) {
    int currentIndex = currentDequeueIndex;
    currentDequeueIndex = nextQueueInRoundRobin(currentDequeueIndex, matchedQueues);
    return currentIndex;
  }

  /**
   * @brief Get the next queue in the round robin.
   * @details If we are currently on the last queue it becomes the first queue.
   * @param index Current queue index.
   * @param matchedQueues The queues to round robin.
   * @return And adjusted val based on the current queue index.
   * @note Suggested return identifier: adjustedCurrentQueue.
   */
  private int nextQueueInRoundRobin(int index, List<BalancedRedisQueue> matchedQueues) {
    if (index >= matchedQueues.size() - 1) {
      return 0;
    }
    return index + 1;
  }

  /**
   * @brief Convert proto provisions into java multimap.
   * @details This conversion is done to more easily check if a key/value exists in the provisions.
   * @param provisions Provisions list to convert.
   * @return The provisions as a set.
   * @note Suggested return identifier: provisionSet.
   */
  private SetMultimap<String, String> toMultimap(List<Platform.Property> provisions) {
    SetMultimap<String, String> set = LinkedHashMultimap.create();
    for (Platform.Property property : provisions) {
      set.put(property.getName(), property.getValue());
    }
    return set;
  }
}
