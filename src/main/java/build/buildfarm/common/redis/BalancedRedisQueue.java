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

package build.buildfarm.common.redis;

import static com.google.common.collect.Iterables.transform;

import build.buildfarm.common.Queue;
import build.buildfarm.common.StringVisitor;
import build.buildfarm.v1test.QueueStatus;
import com.google.common.collect.ImmutableList;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import javax.annotation.Nullable;
import redis.clients.jedis.Connection;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.util.JedisClusterCRC16;

/**
 * @class BalancedRedisQueue
 * @brief A balanced redis queue.
 * @details A balanced redis queue is an implementation of a queue data structure which internally
 *     uses multiple redis nodes to distribute the data across the cluster. Its important to know
 *     that the lifetime of the queue persists before and after the queue data structure is created
 *     (since it exists in redis). Therefore, two redis queues with the same name, would in fact be
 *     the same underlying redis queues.
 */
public class BalancedRedisQueue {
  private static final Duration START_TIMEOUT = Duration.ofSeconds(1);

  private static final Duration MAX_TIMEOUT = Duration.ofSeconds(8);

  /**
   * @field name
   * @brief The unique name of the queue.
   * @details The name is used as a template for the internal queues distributed across nodes.
   *     Hashtags are added to this base name. This name will not contain a redis hashtag.
   */
  private final String name;

  /**
   * @field originalHashtag
   * @brief The original hashtag of the name provided to the queue.
   * @details If the balanced queue is named with a hashtag, we store it, but will not be able to
   *     use it for the internal balanced queues. They will need to have their own hashes that
   *     correlate to particular nodes. However, if the balanced queue is unable to derive hashtags
   *     it will fallback to a single queue. And rely on the original hashtag it was given. If an
   *     original hashtag is not given, this will be empty.
   */
  private final String originalHashtag;

  /**
   * @field maxQueueSize
   * @brief The maximum amount of elements that should be added to the queue.
   * @details This is used to avoid placing too many elements onto the queue at any given time. For
   *     infinitely sized queues, use -1.
   */
  private final int maxQueueSize;

  /**
   * @field queues
   * @brief Internal queues used to distribute data across redis nodes.
   * @details Although these are multiple queues, the balanced redis queue treats them as one in its
   *     interface.
   */
  private final List<String> queues;

  private final QueueDecorator<String> queueDecorator;

  /**
   * @field currentPushQueue
   * @brief The current queue to act push on.
   * @details Used in a round-robin fashion to ensure an even distribution of pushes and appropriate
   *     ordering of pops.
   */
  private int currentPushQueue = 0;

  /**
   * @field currentPopQueue
   * @brief The current queue to act pop on.
   * @details Used in a round-robin fashion to ensure an even distribution of pushes and appropriate
   *     ordering of pops.
   */
  private int currentPopQueue = 0;

  /**
   * @brief Constructor.
   * @details Construct a named redis queue with an established redis cluster.
   * @param name The global name of the queue.
   * @param hashtags Hashtags to distribute queue data.
   * @note Overloaded.
   */
  public BalancedRedisQueue(String name, List<String> hashtags, QueueDecorator queueDecorator) {
    this(name, hashtags, -1, queueDecorator);
  }

  /**
   * @brief Constructor.
   * @details Construct a named redis queue with an established redis cluster.
   * @param name The global name of the queue.
   * @param hashtags Hashtags to distribute queue data.
   * @param maxQueueSize The maximum amount of elements that should be added to the queue.
   * @note Overloaded.
   */
  public BalancedRedisQueue(
      String name, List<String> hashtags, int maxQueueSize, QueueDecorator queueDecorator) {
    this(name, maxQueueSize, createHashedQueues(name, hashtags), queueDecorator);
  }

  public BalancedRedisQueue(
      String name, int maxQueueSize, List<String> queues, QueueDecorator queueDecorator) {
    this.originalHashtag = RedisHashtags.existingHash(name);
    this.name = RedisHashtags.unhashedName(name);
    this.maxQueueSize = maxQueueSize;
    this.queues = queues;
    this.queueDecorator = queueDecorator;
  }

  /**
   * @brief Push a value onto the queue.
   * @details Adds the value into one of the internal backend redis queues.
   * @param val The value to push onto the queue.
   */
  public boolean offer(UnifiedJedis unified, String val) {
    String queue = queues.get(roundRobinPushIndex());
    try (Jedis jedis = getJedisFromKey(unified, queue)) {
      return queueDecorator.decorate(jedis, queue).offer(val);
    }
  }

  /**
   * @brief Push a value onto the queue.
   * @details Adds the value into one of the internal backend redis queues.
   * @param val The value to push onto the queue.
   */
  public boolean offer(UnifiedJedis unified, String val, double priority) {
    String queue = queues.get(roundRobinPushIndex());
    try (Jedis jedis = getJedisFromKey(unified, queue)) {
      return queueDecorator.decorate(jedis, queue).offer(val, priority);
    }
  }

  /**
   * @brief Remove element from dequeue.
   * @details Removes an element from the dequeue and specifies whether it was removed.
   * @param val The value to remove.
   * @return Whether or not the value was removed.
   * @note Suggested return identifier: wasRemoved.
   */
  public boolean removeFromDequeue(UnifiedJedis unified, String val) {
    for (String queue : partialIterationQueueOrder()) {
      try (Jedis jedis = getJedisFromKey(unified, queue)) {
        if (queueDecorator.decorate(jedis, queue).removeFromDequeue(val)) {
          return true;
        }
      }
    }
    return false;
  }

  private String take(Jedis jedis, Queue<String> queue, Duration timeout, ExecutorService service)
      throws InterruptedException {
    return interruptibleRequest(() -> queue.take(timeout), jedis::disconnect, service);
  }

  private <T> T interruptibleRequest(
      Callable<T> command, Runnable onInterrupted, ExecutorService service)
      throws InterruptedException {
    Future<T> reply = service.submit(command);
    return getBlockingReply(reply, onInterrupted);
  }

  private <T> T getBlockingReply(Future<T> reply, Runnable onInterrupted)
      throws InterruptedException {
    InterruptedException interruption = null;
    for (; ; ) {
      try {
        return reply.get();
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (interruption != null) {
          interruption.addSuppressed(cause);
          Thread.currentThread().interrupt();
          throw interruption;
        }
        throw new RuntimeException(cause);
      } catch (InterruptedException e) {
        interruption = e;
        Thread.interrupted();
        reply.cancel(true);
        onInterrupted.run();
      }
    }
  }

  /**
   * @brief Pop element into internal dequeue and return value.
   * @details This pops the element from one queue atomically into an internal list called the
   *     dequeue. It will perform an exponential backoff. Null is returned if the overall backoff
   *     times out.
   * @return The value of the transfered element. null if the thread was interrupted.
   * @note Suggested return identifier: val.
   */
  public String take(UnifiedJedis unified, ExecutorService service) throws InterruptedException {
    // The conditions of this algorithm are as followed:
    // - from a client's perspective we want to block indefinitely.
    //   (so this function should not return null under any normal circumstances.)
    // - from an implementation perspective however, we don't want to block indefinitely on any one
    // internal queue.

    // We choose a strategy that round-robins over the queues in different phases.
    // 1. round-robin each queue with nonblocking calls for 1 cycle
    // 2. switch to continuously round-robin blocking calls that exponentially increase their
    // timeout after each full round
    // 3. continue iterating over each queue at a maximally reached timeout.
    // During all phases of this algorithm we want to be able to interrupt the thread.

    // The fastest thing to do first, is round-robin over every queue with a nonblocking dequeue
    // call.
    // If none of the queues are able to dequeue.  We can move onto a different strategy.
    // (a strategy in which the system appears to be under less load)
    int startQueue = currentPopQueue;
    // end this phase if we have done a full round-robin
    boolean blocking = false;
    // try each of the internal queues with exponential backoff
    Duration currentTimeout = START_TIMEOUT;
    while (true) {
      final String val;
      String queueName = queues.get(roundRobinPopIndex());
      try (Jedis jedis = getJedisFromKey(unified, queueName)) {
        Queue<String> queue = queueDecorator.decorate(jedis, queueName);
        if (blocking) {
          val = take(jedis, queue, currentTimeout, service);
        } else {
          val = queue.poll();
        }
      }
      // return if found
      if (val != null) {
        return val;
      }

      // not quite immediate yet...
      if (Thread.currentThread().isInterrupted()) {
        throw new InterruptedException();
      }

      if (currentPopQueue == startQueue) {
        // advance timeout if blocking on queue and not at max each queue cycle
        if (blocking) {
          currentTimeout = currentTimeout.multipliedBy(2);
          if (currentTimeout.compareTo(MAX_TIMEOUT) > 0) {
            currentTimeout = MAX_TIMEOUT;
          }
        } else {
          blocking = true;
        }
      }
    }
  }

  private static Jedis getJedisFromKey(UnifiedJedis jedis, String name) {
    Connection connection = null;
    if (jedis instanceof JedisCluster) {
      JedisCluster cluster = (JedisCluster) jedis;
      connection = cluster.getConnectionFromSlot(JedisClusterCRC16.getSlot(name));
    } else if (jedis instanceof JedisPooled) {
      JedisPooled pooled = (JedisPooled) jedis;
      connection = pooled.getPool().getResource();
    }
    if (connection == null) {
      throw new IllegalArgumentException(jedis.toString());
    }
    return new Jedis(connection);
  }

  /**
   * @brief Pop element into internal dequeue and return value.
   * @details Null is returned if the queue is empty.
   * @return The value of the transfered element. null if queue is empty or thread was interrupted.
   * @note Suggested return identifier: val.
   */
  public @Nullable String poll(UnifiedJedis unified) {
    String queue = queues.get(roundRobinPopIndex());
    try (Jedis jedis = getJedisFromKey(unified, queue)) {
      return queueDecorator.decorate(jedis, queue).poll();
    }
  }

  /**
   * @brief Get the current pop queue.
   * @details Get the queue that the balanced queue intends to pop from next.
   * @return The queue that the balanced queue intends to pop from next.
   * @note Suggested return identifier: currentPopQueue.
   */
  public String getCurrentPopQueue() {
    return queues.get(currentPopQueue);
  }

  /**
   * @brief Get the current pop queue index.
   * @details Get the index of the queue that the balanced queue intends to pop from next.
   * @return The index of the queue that the balanced queue intends to pop from next.
   * @note Suggested return identifier: currentPopQueueIndex.
   */
  public int getCurrentPopQueueIndex() {
    return currentPopQueue;
  }

  /**
   * @brief Get queue at index.
   * @details Get the internal queue at the specified index.
   * @param index The index to the internal queue (must be in bounds).
   * @return The internal queue found at that index.
   * @note Suggested return identifier: internalQueue.
   */
  public String getInternalQueue(int index) {
    return queues.get(index);
  }

  /**
   * @brief Get dequeue name.
   * @details Get the name of the internal dequeue used by the queue. since each internal queue has
   *     their own dequeue, this name is generic without the hashtag.
   * @return The name of the queue.
   * @note Suggested return identifier: name.
   */
  public String getDequeueName() {
    return name + "_dequeue";
  }

  /**
   * @brief Get name.
   * @details Get the name of the queue. this is the redis key used as base name for internal
   *     queues.
   * @return The base name of the queue.
   * @note Suggested return identifier: name.
   */
  public String getName() {
    return name;
  }

  // annoying that there's no inject/accumulate
  private static long size(Iterable<Long> sizes) {
    long size = 0;
    for (long s : sizes) {
      size += s;
    }
    return size;
  }

  /**
   * @brief Get size.
   * @details Checks the current length of the queue.
   * @return The current length of the queue.
   * @note Suggested return identifier: length.
   */
  public long size(UnifiedJedis unified) {
    // the accumulated size of all of the queues
    return size(sizes(unified));
  }

  private long size(UnifiedJedis unified, String queue) {
    try (Jedis jedis = getJedisFromKey(unified, queue)) {
      return queueDecorator.decorate(jedis, queue).size();
    }
  }

  private Iterable<Long> sizes(UnifiedJedis unified) {
    // this could be done in parallel
    return transform(queues, queue -> size(unified, queue));
  }

  /**
   * @brief Get status information about the queue.
   * @details Helpful for understanding the current load on the queue and how elements are balanced.
   * @return The current status of the queue.
   * @note Suggested return identifier: status.
   */
  public QueueStatus status(UnifiedJedis unified) {
    // get properties
    Iterable<Long> sizes = sizes(unified);

    // build proto
    return QueueStatus.newBuilder()
        .setName(RedisHashtags.hashedName(name, originalHashtag))
        .setSize(size(sizes))
        .addAllInternalSizes(sizes)
        .build();
  }

  /**
   * @brief Visit each element in the queue.
   * @details Enacts a visitor over each element in the queue.
   * @param visitor A visitor for each visited element in the queue.
   */
  public void visit(UnifiedJedis unified, StringVisitor visitor) {
    for (String queue : fullIterationQueueOrder()) {
      try (Jedis jedis = getJedisFromKey(unified, queue)) {
        queueDecorator.decorate(jedis, queue).visit(visitor);
      }
    }
  }

  /**
   * @brief Visit each element in the dequeue.
   * @details Enacts a visitor over each element in the dequeue.
   * @param visitor A visitor for each visited element in the queue.
   */
  public void visitDequeue(UnifiedJedis unified, StringVisitor visitor) {
    for (String queue : fullIterationQueueOrder()) {
      try (Jedis jedis = getJedisFromKey(unified, queue)) {
        queueDecorator.decorate(jedis, queue).visitDequeue(visitor);
      }
    }
  }

  /**
   * @brief Check that the internal queues have evenly distributed the values.
   * @details We are checking that the size of all the internal queues are the same. This means, the
   *     balanced queue will be evenly distributed on every n elements pushed, where n is the number
   *     of internal queues.
   * @return Whether or not the queues values are evenly distributed by internal queues.
   * @note Suggested return identifier: isEvenlyDistributed.
   */
  public boolean isEvenlyDistributed(UnifiedJedis unified) {
    long size = -1;
    for (long queueSize : sizes(unified)) {
      if (size != -1 && queueSize != size) {
        return false;
      }
      size = queueSize;
    }
    return true;
  }

  /**
   * @brief Whether or not more elements can be added to the queue based on the queue's configured
   *     max size.
   * @details Compares the size of the queue to configured max size. Queues may be configured to be
   *     infinite in size.
   * @param jedis Jedis cluster client.
   * @return Whether are not a new element can be added to the queue based on its current size.
   */
  public boolean canQueue(UnifiedJedis jedis) {
    return maxQueueSize < 0 || size(jedis) < maxQueueSize;
  }

  /**
   * @brief Create multiple queues for each of the hashes given.
   * @details Create the multiple queues that will act as a single balanced queue.
   * @param name The global name of the queue.
   * @param hashtags Hashtags to distribute queue data.
   */
  private static List<String> createHashedQueues(String name, List<String> hashtags) {
    String unhashedName = RedisHashtags.unhashedName(name);
    ImmutableList.Builder<String> queues = ImmutableList.builder();
    // if there were no hashtags, we'll create a single internal queue
    // so that the balanced redis queue can still function.
    // we'll use the basename provided to create the single internal queue and use the original
    // hashtag provided.
    // if there was no original hashtag, we will use a hashtag that corresponds to the first slot.
    // note: we must build the balanced queues internal queue with a hashtag because it will dequeue
    // to the same redis slot.
    if (hashtags.isEmpty()) {
      String originalHashtag = RedisHashtags.existingHash(name);
      hashtags = ImmutableList.of(originalHashtag.isEmpty() ? "06S" : originalHashtag);
    }
    // create an internal queue for each of the provided hashtags
    for (String hashtag : hashtags) {
      queues.add(RedisHashtags.hashedName(unhashedName, hashtag));
    }
    return queues.build();
  }

  /**
   * @brief Get the current queue index for round-robin pushing.
   * @details Adjusts the round-robin index for next call.
   * @return The current round-robin index.
   * @note Suggested return identifier: queueIndex.
   */
  private synchronized int roundRobinPushIndex() {
    int currentIndex = currentPushQueue;
    currentPushQueue = nextQueueInRoundRobin(currentPushQueue);
    return currentIndex;
  }

  /**
   * @brief Get the current queue index for round-robin popping.
   * @details Adjusts the round-robin index for next call.
   * @return The current round-robin index.
   * @note Suggested return identifier: queueIndex.
   */
  private synchronized int roundRobinPopIndex() {
    int currentIndex = currentPopQueue;
    currentPopQueue = nextQueueInRoundRobin(currentPopQueue);
    return currentIndex;
  }

  /**
   * @brief Get the next queue in the round robin.
   * @details If we are currently on the last queue it becomes the first queue.
   * @param index Current queue index.
   * @return And adjusted val based on the current queue index.
   * @note Suggested return identifier: adjustedCurrentQueue.
   */
  private int nextQueueInRoundRobin(int index) {
    if (index >= queues.size() - 1) {
      return 0;
    }
    return index + 1;
  }

  /**
   * @brief List of queues in a particular order for full iteration over all of the queues.
   * @details An ordered list of queues for operations that assume to traverse over all of the
   *     queues. Some operations like clear() / size() require calling methods on all of the
   *     internal queues. For those cases, this function represents the desired order of the queues.
   * @return An ordered list of queues.
   * @note Suggested return identifier: queues.
   */
  private List<String> fullIterationQueueOrder() {
    // if we are going to iterate over all of the queues
    // there will be no noticeable side effects from the order
    return queues;
  }

  /**
   * @brief List of queues in a particular order for a possibly partial iteration over all of the
   *     queues.
   * @details An ordered list of queues for operations that may end early without needing to perform
   *     the operation on all of the internal queues. Some operations like exists() / remove() can
   *     return early without processing over all of the internal queues. For those cases, this
   *     function represents the desired order of the queues.
   * @return An ordered list of queues.
   * @note Suggested return identifier: queues.
   */
  private List<String> partialIterationQueueOrder() {
    // to improve cpu utilization, we can try randomizing
    // the order we traverse the internal queues for operations
    // that may return early
    List<String> randomQueues = new ArrayList<>(queues);
    Collections.shuffle(randomQueues);
    return randomQueues;
  }
}
