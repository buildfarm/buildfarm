// Copyright 2020 The Buildfarm Authors. All rights reserved.
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

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Lists.newArrayList;
import static redis.clients.jedis.params.ScanParams.SCAN_POINTER_START;

import build.bazel.remote.execution.v2.Platform;
import build.buildfarm.common.Visitor;
import build.buildfarm.common.redis.BalancedRedisQueue;
import build.buildfarm.common.redis.BalancedRedisQueue.BalancedQueueEntry;
import build.buildfarm.common.redis.ProvisionedRedisQueue;
import build.buildfarm.v1test.OperationQueueStatus;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.QueueStatus;
import build.buildfarm.worker.resources.LocalResourceSet;
import build.buildfarm.worker.resources.LocalResourceSetUtils;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.java.Log;
import redis.clients.jedis.AbstractPipeline;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.resps.ScanResult;

/**
 * @class ExecutionQueue
 * @brief The operation queue of the shard backplane.
 * @details The operation queue can be split into multiple queues according to platform execution
 *     information.
 */
@Log
public class ExecutionQueue {
  private static final Duration START_TIMEOUT = Duration.ofSeconds(1);

  private static final Duration MAX_TIMEOUT = Duration.ofSeconds(8);

  public record ExecutionQueueEntry(
      BalancedRedisQueue queue, BalancedQueueEntry balancedQueueEntry, QueueEntry queueEntry) {}

  /**
   * @field maxQueueSize
   * @brief The maximum amount of elements that should be added to the queue.
   * @details This is used to avoid placing too many elements onto the queue at any given time. For
   *     infinitely sized queues, use -1.
   */
  @Getter private final int maxQueueSize;

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
  public ExecutionQueue(List<ProvisionedRedisQueue> queues) {
    this.queues = queues;
    this.maxQueueSize = -1; // infinite size
  }

  /**
   * @brief Constructor.
   * @details Construct the operation queue with various provisioned redis queues.
   * @param queues Provisioned queues.
   * @param maxQueueSize The maximum amount of elements that should be added to the queue.
   */
  public ExecutionQueue(List<ProvisionedRedisQueue> queues, int maxQueueSize) {
    this.queues = queues;
    this.maxQueueSize = maxQueueSize;
  }

  private static Visitor<BalancedQueueEntry> createExecutionQueueVisitor(
      UnifiedJedis jedis, BalancedRedisQueue queue, Visitor<ExecutionQueueEntry> visitor) {
    return new Visitor<>() {
      @Override
      public void visit(BalancedQueueEntry balancedQueueEntry) {
        String entry = balancedQueueEntry.value();
        QueueEntry.Builder queueEntry = QueueEntry.newBuilder();
        try {
          JsonFormat.parser().merge(entry, queueEntry);
          visitor.visit(new ExecutionQueueEntry(queue, balancedQueueEntry, queueEntry.build()));
        } catch (InvalidProtocolBufferException e) {
          log.log(Level.SEVERE, "invalid QueueEntry json: " + entry, e);
          queue.removeFromDequeue(jedis, balancedQueueEntry);
        }
      }
    };
  }

  /**
   * @brief Visit each element in the dequeue.
   * @details Enacts a visitor over each element in the dequeue.
   * @param jedis Jedis cluster client.
   * @param visitor A visitor for each visited element in the queue.
   */
  public void visitDequeue(UnifiedJedis jedis, Visitor<ExecutionQueueEntry> visitor) {
    for (ProvisionedRedisQueue provisionedQueue : queues) {
      BalancedRedisQueue queue = provisionedQueue.queue();
      queue.visitDequeue(jedis, createExecutionQueueVisitor(jedis, queue, visitor));
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
  public static boolean removeFromDequeue(UnifiedJedis jedis, ExecutionQueueEntry entry) {
    return entry.queue().removeFromDequeue(jedis, entry.balancedQueueEntry());
  }

  public static void removeFromDequeue(AbstractPipeline pipeline, ExecutionQueueEntry entry) {
    entry.queue().removeFromDequeue(pipeline, entry.balancedQueueEntry());
  }

  /**
   * @brief Visit each element in the queue.
   * @details Enacts a visitor over each element in the queue.
   * @param jedis Jedis cluster client.
   * @param visitor A visitor for each visited element in the queue.
   */
  public void visit(UnifiedJedis jedis, Visitor<ExecutionQueueEntry> visitor) {
    for (ProvisionedRedisQueue provisionedQueue : queues) {
      BalancedRedisQueue queue = provisionedQueue.queue();
      queue.visit(jedis, createExecutionQueueVisitor(jedis, queue, visitor));
    }
  }

  /**
   * @brief Get size.
   * @details Checks the current length of the queue.
   * @param jedis Jedis cluster client.
   * @return The current length of the queue.
   * @note Suggested return identifier: length.
   */
  public long size(UnifiedJedis jedis) {
    // the accumulated size of all of the queues
    return queues.stream().mapToInt(i -> (int) i.queue().size(jedis)).sum();
  }

  public Supplier<Long> size(AbstractPipeline pipeline) {
    List<Supplier<Long>> sizes =
        queues.stream().map(q -> q.queue().size(pipeline)).collect(Collectors.toList());
    return new Supplier<>() {
      @Override
      public Long get() {
        return sizes.stream().map(Supplier::get).mapToLong(Long::longValue).sum();
      }
    };
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
      UnifiedJedis jedis, List<Platform.Property> provisions, String val, int priority) {
    BalancedRedisQueue queue = chooseEligibleQueue(provisions);
    queue.offer(jedis, val, (double) priority);
  }

  public ExecutionQueueEntry take(
      UnifiedJedis jedis,
      List<ProvisionedRedisQueue> queues,
      LocalResourceSet resourceSet,
      ExecutorService service)
      throws InterruptedException {
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
    int startQueue = currentDequeueIndex;
    // end this phase if we have done a full round-robin
    boolean blocking = false;
    // try each of the internal queues with exponential backoff
    Duration currentTimeout = START_TIMEOUT;
    while (true) {
      BalancedQueueEntry balancedQueueEntry = null;
      int index = roundRobinPopIndex(queues);
      ProvisionedRedisQueue provisionedQueue = queues.get(index);
      BalancedRedisQueue queue = provisionedQueue.queue();
      if (!provisionedQueue.isExhausted(LocalResourceSetUtils.exhausted(resourceSet))) {
        if (blocking) {
          balancedQueueEntry = queue.takeAny(jedis, currentTimeout, service);
        } else {
          balancedQueueEntry = queue.pollAny(jedis);
        }
      }
      // TODO need logic to determine if _all_ queues are currently exhausted

      // return if found
      if (balancedQueueEntry != null) {
        try {
          QueueEntry.Builder queueEntryBuilder = QueueEntry.newBuilder();
          JsonFormat.parser().merge(balancedQueueEntry.value(), queueEntryBuilder);
          QueueEntry queueEntry = queueEntryBuilder.build();

          return new ExecutionQueueEntry(queue, balancedQueueEntry, queueEntry);
        } catch (InvalidProtocolBufferException e) {
          queue.removeFromDequeue(jedis, balancedQueueEntry);
          log.log(Level.SEVERE, "error parsing queue entry", e);
        }
      }

      // not quite immediate yet...
      if (Thread.currentThread().isInterrupted()) {
        throw new InterruptedException();
      }

      if (currentDequeueIndex == startQueue) {
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
  public ExecutionQueueEntry dequeue(
      UnifiedJedis jedis,
      List<Platform.Property> provisions,
      LocalResourceSet resourceSet,
      ExecutorService service)
      throws InterruptedException {
    // Select all matched queues, and attempt dequeuing via round-robin.
    List<ProvisionedRedisQueue> queues = chooseEligibleQueues(provisions);
    checkState(!queues.isEmpty());
    // Keep iterating over matched queues until we find one that is non-empty and provides a
    // dequeued value.
    return take(jedis, queues, resourceSet, service);
  }

  /**
   * @brief Get status information about the queue.
   * @details Helpful for understanding the current load on the queue and how elements are balanced.
   * @param jedis Jedis cluster client.
   * @return The current status of the queue.
   * @note Overloaded.
   * @note Suggested return identifier: status.
   */
  public Supplier<OperationQueueStatus> status(AbstractPipeline pipeline) {
    Supplier<Long> size = size(pipeline);
    List<Supplier<QueueStatus>> provisions =
        queues.stream()
            .map(provisionedQueue -> provisionedQueue.queue().status(pipeline))
            .collect(Collectors.toList());
    return new Supplier<>() {
      @Override
      public OperationQueueStatus get() {
        return OperationQueueStatus.newBuilder()
            .setSize(size.get())
            .addAllProvisions(provisions.stream().map(Supplier::get).collect(Collectors.toList()))
            .build();
      }
    };
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
  public boolean canQueue(UnifiedJedis jedis) {
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

  /**
   * @brief Choose an eligible queues based on given properties.
   * @details We use the platform execution properties of a queue entry to determine the appropriate
   *     queues. If there no eligible queues, an exception is thrown.
   * @param provisions Provisions to check that requirements are met.
   * @return The chosen queues.
   * @note Suggested return identifier: queues.
   */
  private List<ProvisionedRedisQueue> chooseEligibleQueues(List<Platform.Property> provisions) {
    List<ProvisionedRedisQueue> eligibleQueues =
        queues.stream()
            .filter(provisionedQueue -> provisionedQueue.isEligible(toMultimap(provisions)))
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
        "There are no eligible queues for the provided execution requirements. One solution to is"
            + " to configure a provision queue with no requirements which would be eligible to all"
            + " operations. See"
            + " https://buildfarm.github.io/buildfarm/docs/architecture/queues/"
            + " for details. "
            + eligibilityResults);
  }

  /**
   * @brief Get the current queue index for round-robin dequeues.
   * @details Adjusts the round-robin index for next call.
   * @param matchedQueues The queues to round robin.
   * @return The current round-robin index.
   * @note Suggested return identifier: queueIndex.
   */
  private int roundRobinPopIndex(List<?> matchedQueues) {
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
  private int nextQueueInRoundRobin(int index, List<?> matchedQueues) {
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
  private static SetMultimap<String, String> toMultimap(List<Platform.Property> provisions) {
    SetMultimap<String, String> set = LinkedHashMultimap.create();
    for (Platform.Property property : provisions) {
      set.put(property.getName(), property.getValue());
    }
    return set;
  }

  private static QueueEntry parse(String json) {
    QueueEntry.Builder queueEntry = QueueEntry.newBuilder();
    try {
      JsonFormat.parser().merge(json, queueEntry);
    } catch (InvalidProtocolBufferException e) {
      log.log(Level.SEVERE, "invalid QueueEntry json: " + json, e);
    }
    return queueEntry.build();
  }

  public ScanResult<ExecutionQueueEntry> scan(
      UnifiedJedis jedis, String queueCursor, int count, String match) {
    int queueIndex = queueCursor.indexOf('{') + 1;
    String currentQueue = null;
    if (queueIndex > 0) {
      int queueEnd = queueCursor.indexOf('}');
      currentQueue = queueCursor.substring(queueIndex, queueEnd);
      queueCursor = queueCursor.substring(queueEnd + 1);
    }
    Iterator<ProvisionedRedisQueue> queueIter = queues.iterator();

    BalancedRedisQueue queue = null;
    while (currentQueue != null && queueIter.hasNext()) {
      queue = queueIter.next().queue();
      if (queue.getName().equals(currentQueue)) {
        break;
      }
    }

    if (currentQueue != null && !currentQueue.equals(queue.getName())) {
      return new ScanResult<>(SCAN_POINTER_START, new ArrayList<>());
    }

    List<ExecutionQueueEntry> result = new ArrayList<>(count);
    while (result.size() < count) {
      if (currentQueue == null || queueCursor.equals(SCAN_POINTER_START)) {
        if (!queueIter.hasNext()) {
          break;
        }
        queue = queueIter.next().queue();
        currentQueue = queue.getName();
      }
      final BalancedRedisQueue entryQueue = queue;
      ScanResult<BalancedQueueEntry> scanResult =
          queue.scan(jedis, queueCursor, count - result.size(), match);
      queueCursor = scanResult.getCursor();
      result.addAll(
          newArrayList(
              transform(
                  scanResult.getResult(),
                  balancedQueueEntry ->
                      new ExecutionQueueEntry(
                          entryQueue, balancedQueueEntry, parse(balancedQueueEntry.value())))));
    }

    if (queueCursor.equals(SCAN_POINTER_START)) {
      currentQueue = null;
      if (queueIter.hasNext()) {
        currentQueue = queueIter.next().queue().getName();
      }
    }
    String nextCursor = SCAN_POINTER_START;
    if (currentQueue != null) {
      nextCursor = "{" + currentQueue + "}" + queueCursor;
    }
    return new ScanResult<>(nextCursor, result);
  }
}
