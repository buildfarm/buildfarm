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

import build.buildfarm.common.StringVisitor;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import redis.clients.jedis.Jedis;

/**
 * @class RedisPriorityQueue
 * @brief A redis priority queue.
 * @details A redis queue is an implementation of a queue data structure which internally uses redis
 *     to store and distribute the data. Its important to know that the lifetime of the queue
 *     persists before and after the queue data structure is created (since it exists in redis).
 *     Therefore, two redis queues with the same name, would in fact be the same underlying redis
 *     queue.
 */
public class RedisPriorityQueue extends QueueInterface {
  /**
   * @field name
   * @brief The unique name of the queue.
   * @details The name is used by the redis cluster client to access the queue data. If two queues
   *     had the same name, they would be instances of the same underlying redis queue.
   */
  private final String name;

  private final String script;
  private Timestamp time;
  private final List<String> keys;
  private final long pollIntervalMillis;
  private static long defaultPollIntervalMillis = 100;

  /**
   * @brief Constructor.
   * @details Construct a named redis queue with an established redis cluster.
   * @param name The global name of the queue.
   */
  public RedisPriorityQueue(String name) {
    this(name, new Timestamp(), defaultPollIntervalMillis);
  }

  /**
   * @brief Constructor.
   * @details Construct a named redis queue with an established redis cluster. Used to ease the
   *     testing of the order of the queued actions
   * @param name The global name of the queue.
   * @param time Timestamp of the operation.
   */
  public RedisPriorityQueue(String name, Timestamp time) {
    this(name, time, defaultPollIntervalMillis);
  }

  /**
   * @brief Constructor.
   * @details Construct a named redis queue with an established redis cluster. Used to ease the
   *     testing of the order of the queued actions
   * @param name The global name of the queue.
   * @param pollIntervalMillis pollInterval to use when dqueuing from redis.
   */
  public RedisPriorityQueue(String name, long pollIntervalMillis) {
    this(name, new Timestamp(), pollIntervalMillis);
  }

  /**
   * @brief Constructor.
   * @details Construct a named redis queue with an established redis cluster. Used to ease the
   *     testing of the order of the queued actions
   * @param name The global name of the queue.
   * @param time Timestamp of the operation.
   * @param pollIntervalMillis pollInterval to use when dqueuing from redis.
   */
  public RedisPriorityQueue(String name, Timestamp time, long pollIntervalMillis) {
    this.name = name;
    this.time = time;
    this.keys = Arrays.asList(name);
    this.script = getLuaScript();
    this.pollIntervalMillis = pollIntervalMillis;
  }

  /**
   * @brief Push a value onto the queue with default priority of 0.
   * @details Adds the value into the backend ordered set.
   * @param val The value to push onto the priority queue.
   */
  @Override
  public void push(Jedis jedis, String val) {
    push(jedis, val, 0);
  }

  /**
   * @brief Push a value onto the queue with specified priority.
   * @details Adds the value into the backend redis ordered set, with timestamp primary insertion to
   *     guarantee FIFO within a single priority level
   * @param val The value to push onto the priority queue.
   * @param priority The priority of action 0 means highest
   */
  @Override
  public void push(Jedis jedis, String val, double priority) {
    jedis.zadd(name, priority, time.getNanos() + ":" + val);
  }

  /**
   * @brief Remove element from dequeue.
   * @details Removes an element from the dequeue and specifies whether it was removed.
   * @param val The value to remove.
   * @return Whether or not the value was removed.
   * @note Suggested return identifier: wasRemoved.
   */
  @Override
  public boolean removeFromDequeue(Jedis jedis, String val) {
    return jedis.lrem(getDequeueName(), -1, val) != 0;
  }

  /**
   * @brief Remove all elements that match from queue.
   * @details Removes all matching elements from the queue and specifies whether it was removed.
   * @param val The value to remove.
   * @return Whether or not the value was removed.
   * @note Suggested return identifier: wasRemoved.
   */
  @Override
  public boolean removeAll(Jedis jedis, String val) {
    return jedis.zrem(name, val) != 0;
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
  public String dequeue(Jedis jedis, Duration timeout, ExecutorService service)
      throws InterruptedException {
    int maxAttempts = Math.max(1, (int) (timeout.toMillis() / pollIntervalMillis));
    List<String> args = Arrays.asList(name, getDequeueName(), "true");
    String val;
    for (int i = 0; i < maxAttempts; ++i) {
      Object obj_val = jedis.eval(script, keys, args);
      val = String.valueOf(obj_val);
      if (!isEmpty(val)) {
        return val;
      }
      Thread.sleep(pollIntervalMillis);
    }
    return null;
  }

  /**
   * @brief Pop element into internal dequeue and return value.
   * @details This pops the element from one queue atomically into an internal list called the
   *     dequeue. It does not block and null is returned if there is nothing to dequeue.
   * @return The value of the transfered element. null if nothing was dequeued.
   * @note Suggested return identifier: val.
   */
  @Override
  public String nonBlockingDequeue(Jedis jedis) throws InterruptedException {
    List<String> args = Arrays.asList(name, getDequeueName());
    Object obj_val = jedis.eval(script, keys, args);
    String val = String.valueOf(obj_val);
    if (!isEmpty(val)) {
      return val;
    }
    if (Thread.currentThread().isInterrupted()) {
      throw new InterruptedException();
    }
    return null;
  }

  /**
   * @brief Get name.
   * @details Get the name of the queue. this is the redis key used for the list.
   * @return The name of the queue.
   * @note Suggested return identifier: name.
   */
  @Override
  public String getName() {
    return name;
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
  @Override
  public long size(Jedis jedis) {
    return jedis.zcard(name);
  }

  /**
   * @brief Visit each element in the queue.
   * @details Enacts a visitor over each element in the queue.
   * @param visitor A visitor for each visited element in the queue.
   * @note Overloaded.
   */
  @Override
  public void visit(Jedis jedis, StringVisitor visitor) {
    visit(jedis, name, visitor);
  }

  /**
   * @brief Visit each element in the dequeue.
   * @details Enacts a visitor over each element in the dequeue.
   * @param visitor A visitor for each visited element in the queue.
   */
  @Override
  public void visitDequeue(Jedis jedis, StringVisitor visitor) {
    int listPageSize = 10000;
    int index = 0;
    int nextIndex = listPageSize;
    List<String> entries;

    do {
      entries = jedis.lrange(getDequeueName(), index, nextIndex - 1);
      for (String entry : entries) {
        visitor.visit(entry);
      }
      index = nextIndex;
      nextIndex += entries.size();
    } while (entries.size() == listPageSize);
  }

  /**
   * @brief Visit each element in the queue via queue name.
   * @details Enacts a visitor over each element in the queue.
   * @param queueName The name of the queue to visit.
   * @param visitor A visitor for each visited element in the queue.
   * @note Overloaded.
   */
  private void visit(Jedis jedis, String queueName, StringVisitor visitor) {
    int listPageSize = 10000;
    int index = 0;
    int nextIndex = listPageSize;
    List<String> entries;

    do {
      entries = jedis.zrange(queueName, index, nextIndex - 1);
      for (String entry : entries) {
        // Clear the appended timestamp
        visitor.visit(entry.replaceFirst("^([0-9]+):(?!$)", ""));
      }
      index = nextIndex;
      nextIndex += entries.size();
    } while (entries.size() == listPageSize);
  }

  /**
   * @brief Adds additional functionality to the jedis client.
   * @details Load the custom lua script so we can have zpoplpush functionality in our container.
   */
  private String getLuaScript() {
    // We return the lua code in-line to avoid any build complexities having to bundle lua code with
    // the buildfarm artifacts.  Lua code is fed to redis via the eval call.
    return String.join(
        "\n",
        "local zset = ARGV[1]",
        "local deqName = ARGV[2]",
        "local val = ''",
        "local function isempty(s)",
        "   return s == nil or s == ''",
        "end",
        "assert(not isempty(zset), 'ERR1: zset missing')",
        "assert(not isempty(deqName), 'ERR2: dequeue missing')",
        "local pped = redis.call('ZRANGE', zset, 0, 0)",
        "if next(pped) ~= nil then",
        "  for _,item in ipairs(pped) do",
        "    val = string.gsub(item, '^%d*:', '')",
        "    redis.call('ZREM', zset, item)",
        "    redis.call('LPUSH', deqName, val)",
        "  end",
        "end",
        "return val");
  }

  /**
   * @brief Implement handy isEmpty method.
   * @details Compare the value for null, (empty string) or "null" string. For some reason
   *     redis/jedis returns 'null' as string
   */
  private boolean isEmpty(String val) {
    return val == null || val.isEmpty() || val.equalsIgnoreCase("null");
  }
}
