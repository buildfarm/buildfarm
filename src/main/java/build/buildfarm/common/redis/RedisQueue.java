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

import static redis.clients.jedis.args.ListDirection.LEFT;
import static redis.clients.jedis.args.ListDirection.RIGHT;

import build.buildfarm.common.Queue;
import build.buildfarm.common.StringVisitor;
import com.google.common.collect.ImmutableList;
import java.time.Duration;
import java.util.List;
import redis.clients.jedis.Jedis;

/**
 * @class RedisQueue
 * @brief A redis queue.
 * @details A redis queue is an implementation of a queue data structure which internally uses redis
 *     to store and distribute the data. It's important to know that the lifetime of the queue
 *     persists before and after the queue data structure is created (since it exists in redis).
 *     Therefore, two redis queues with the same name and redis service, would in fact be the same
 *     underlying redis queue.
 */
public class RedisQueue implements Queue<String> {
  public static final int UNLIMITED_QUEUE_DEPTH = -1;

  private static final int defaultListPageSize = 10000;

  public static Queue decorate(Jedis jedis, String name, int maxQueueDepth) {
    return new RedisQueue(jedis, name, maxQueueDepth, defaultListPageSize);
  }

  private static double toRedisTimeoutSeconds(Duration timeout) {
    return timeout.getSeconds() + timeout.getNano() / 1e9;
  }

  private final Jedis jedis;

  /**
   * @field name
   * @brief The unique name of the queue.
   * @details The name is used by the redis cluster client to access the queue data. If two queues
   *     had the same name, they would be instances of the same underlying redis queue.
   */
  private final String name;

  private final int maxNodeQueueDepth;

  private final RedisLuaScript pushScript;

  private final int listPageSize;

  /**
   * @brief Constructor.
   * @details Construct a named redis queue with an established redis cluster.
   * @param name The global name of the queue.
   * @param maxNodeQueueDepth The maximum size the queue is allowed to grow to.
   */
  public RedisQueue(Jedis jedis, String name) {
    this(jedis, name, UNLIMITED_QUEUE_DEPTH, defaultListPageSize);
  }

  /**
   * @brief Constructor.
   * @details Construct a named redis queue with an established redis cluster.
   * @param name The global name of the queue.
   * @param maxNodeQueueDepth The maximum size the queue is allowed to grow to.
   */
  public RedisQueue(Jedis jedis, String name, int maxNodeQueueDepth) {
    this(jedis, name, maxNodeQueueDepth, defaultListPageSize);
  }

  public RedisQueue(Jedis jedis, String name, int maxNodeQueueDepth, int listPageSize) {
    this.jedis = jedis;
    this.name = name;
    this.maxNodeQueueDepth = maxNodeQueueDepth;
    this.pushScript = new RedisLuaScript(getPushLuaScript());
    this.listPageSize = listPageSize;
  }

  /**
   * @brief Push a value onto the queue.
   * @details Adds the value into the backend redis queue.
   * @param val The value to push onto the queue.
   */
  @Override
  public boolean offer(String val) {
    return offer(val, 1);
  }

  /**
   * @brief Push a value onto the queue.
   * @details Adds the value into the backend redis queue.
   * @param val The value to push onto the queue.
   */
  @Override
  public boolean offer(String val, double priority) {
    if (maxNodeQueueDepth == UNLIMITED_QUEUE_DEPTH) {
      jedis.lpush(name, val);
      return true;
    }
    ImmutableList<String> keys = ImmutableList.of(name);
    ImmutableList<String> args = ImmutableList.of(String.valueOf(maxNodeQueueDepth), val);
    return String.valueOf(pushScript.eval(jedis, keys, args)).equals("t");
  }

  /**
   * @brief Push a value onto the queue.
   * @details Adds the value into the backend redis queue.
   * @param val The value to push onto the queue.
   */
  @Override
  public void unboundedAdd(String val, double priority) {
    jedis.lpush(name, val);
  }

  /**
   * @brief Remove element from dequeue.
   * @details Removes an element from the dequeue and specifies whether it was removed.
   * @param val The value to remove.
   * @return Whether the value was removed.
   * @note Suggested return identifier: wasRemoved.
   */
  @Override
  public boolean removeFromDequeue(String val) {
    return jedis.lrem(getDequeueName(), -1, val) != 0;
  }

  /**
   * @brief Remove all elements that match from queue.
   * @details Removes all matching elements from the queue and specifies whether it was removed.
   * @param val The value to remove.
   * @return Whether the value was removed.
   * @note Suggested return identifier: wasRemoved.
   */
  public boolean removeAll(String val) {
    return jedis.lrem(name, 0, val) != 0;
  }

  /**
   * @brief Pop element into internal dequeue and return value.
   * @details This pops the element from one queue atomically into an internal list called the
   *     dequeue. It will wait until the timeout has expired. Null is returned if the timeout has
   *     expired.
   * @param timeout Timeout to wait if there is no item to dequeue.
   * @return The value of the transfered element. null if the thread was interrupted.
   * @note Overloaded.
   * @note Suggested return identifier: val.
   */
  @Override
  public String take(Duration timeout) {
    return jedis.blmove(name, getDequeueName(), RIGHT, LEFT, toRedisTimeoutSeconds(timeout));
  }

  /**
   * @brief Pop element into internal dequeue and return value.
   * @details This pops the element from one queue atomically into an internal list called the
   *     dequeue. It does not block and null is returned if there is nothing to dequeue.
   * @return The value of the transfered element. null if nothing was dequeued.
   * @note Suggested return identifier: val.
   */
  @Override
  public String poll() {
    return jedis.lmove(name, getDequeueName(), RIGHT, LEFT);
  }

  /**
   * @brief Get dequeue name.
   * @details Get the name of the internal dequeue used by the queue. this is the redis key used for
   *     the list.
   * @return The name of the queue.
   * @note Suggested return identifier: name.
   */
  public String getDequeueName() {
    return name + "_dequeue";
  }

  /**
   * @brief Get size.
   * @details Checks the current length of the queue.
   * @return The current length of the queue.
   * @note Suggested return identifier: length.
   */
  public long size() {
    return jedis.llen(name);
  }

  /**
   * @brief Visit each element in the queue.
   * @details Enacts a visitor over each element in the queue.
   * @param visitor A visitor for each visited element in the queue.
   * @note Overloaded.
   */
  public void visit(StringVisitor visitor) {
    visit(name, visitor);
  }

  /**
   * @brief Visit each element in the dequeue.
   * @details Enacts a visitor over each element in the dequeue.
   * @param visitor A visitor for each visited element in the queue.
   */
  public void visitDequeue(StringVisitor visitor) {
    visit(getDequeueName(), visitor);
  }

  /**
   * @brief Visit each element in the queue via queue name.
   * @details Enacts a visitor over each element in the queue.
   * @param queueName The name of the queue to visit.
   * @param visitor A visitor for each visited element in the queue.
   * @note Overloaded.
   */
  private void visit(String queueName, StringVisitor visitor) {
    int index = 0;
    int nextIndex = listPageSize;
    List<String> entries;

    do {
      entries = jedis.lrange(queueName, index, nextIndex - 1);
      for (String entry : entries) {
        visitor.visit(entry);
      }
      index = nextIndex;
      nextIndex += entries.size();
    } while (entries.size() == listPageSize);
  }

  private String getPushLuaScript() {
    return String.join(
        "\n",
        "local maxNodeQueueDepth = tonumber(ARGV[1])",
        "local queueSize = redis.call('LLEN', KEYS[1])",
        "if queueSize < maxNodeQueueDepth then",
        "  redis.call('LPUSH', KEYS[1], ARGV[2])",
        "  return 't'",
        "end",
        "return 'f'");
  }
}
