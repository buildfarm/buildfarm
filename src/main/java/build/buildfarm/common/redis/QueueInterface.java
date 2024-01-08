// Copyright 2020-2022 The Bazel Authors. All rights reserved.
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

import build.buildfarm.common.StringVisitor;
import java.util.concurrent.ExecutorService;
import redis.clients.jedis.Jedis;

/**
 * @class QueueInterface
 * @brief A redis queue interface.
 */
public abstract class QueueInterface {
  /**
   * @brief Push a value onto the queue with default priority of 1.
   * @details Adds the value into the backend rdered set.
   * @param val The value to push onto the priority queue.
   */
  abstract void push(Jedis jedis, String val);

  /**
   * @brief Push a value onto the queue with defined priority.
   * @details Adds the value into the backend rdered set.
   * @param val The value to push onto the priority queue.
   */
  abstract void push(Jedis jedis, String val, double priority);

  /**
   * @brief Remove element from dequeue.
   * @details Removes an element from the dequeue and specifies whether it was removed.
   * @param val The value to remove.
   * @return Whether or not the value was removed.
   * @note Suggested return identifier: wasRemoved.
   */
  abstract boolean removeFromDequeue(Jedis jedis, String val);

  /**
   * @brief Remove all elements that match from queue.
   * @details Removes all matching elements from the queue and specifies whether it was removed.
   * @param val The value to remove.
   * @return Whether or not the value was removed.
   * @note Suggested return identifier: wasRemoved.
   */
  abstract boolean removeAll(Jedis jedis, String val);

  /**
   * @brief Pop element into internal dequeue and return value.
   * @details This pops the element from one queue atomically into an internal list called the
   *     dequeue. It will wait until the timeout has expired. Null is returned if the timeout has
   *     expired. It is up to the caller to maintain the Jedis object and ensure it is valid for the
   *     queue operations.
   * @param timeout_ms Timeout to wait if there is no item to dequeue. (units: milliseconds (ms))
   * @return The value of the transfered element. null if the thread was interrupted.
   * @note Overloaded.
   * @note Suggested return identifier: val.
   */
  abstract String dequeue(Jedis jedis, int timeout_ms, ExecutorService service)
      throws InterruptedException;

  /**
   * @brief Pop element into internal dequeue and return value.
   * @details This pops the element from one queue atomically into an internal list called the
   *     dequeue. It does not block and null is returned if there is nothing to dequeue.
   * @return The value of the transfered element. null if nothing was dequeued.
   * @note Suggested return identifier: val.
   */
  abstract String nonBlockingDequeue(Jedis jedis) throws InterruptedException;

  /**
   * @brief Get name.
   * @details Get the name of the queue. this is the redis key used for the list.
   * @return The name of the queue.
   * @note Suggested return identifier: name.
   */
  abstract String getName();

  /**
   * @brief Get dequeue name.
   * @details Get the name of the internal dequeue used by the queue. this is the redis key used for
   *     the list.
   * @return The name of the queue.
   * @note Suggested return identifier: name.
   */
  abstract String getDequeueName();

  /**
   * @brief Get size.
   * @details Checks the current length of the queue.
   * @return The current length of the queue.
   * @note Suggested return identifier: length.
   */
  abstract long size(Jedis jedis);

  /**
   * @brief Visit each element in the queue.
   * @details Enacts a visitor over each element in the queue.
   * @param visitor A visitor for each visited element in the queue.
   * @note Overloaded.
   */
  abstract void visit(Jedis jedis, StringVisitor visitor);

  /**
   * @brief Visit each element in the dequeue.
   * @details Enacts a visitor over each element in the dequeue.
   * @param visitor A visitor for each visited element in the queue.
   */
  abstract void visitDequeue(Jedis jedis, StringVisitor visitor);
}
