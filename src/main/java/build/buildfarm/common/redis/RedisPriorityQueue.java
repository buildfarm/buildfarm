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

package build.buildfarm.common.redis;

import static com.google.common.collect.Lists.newArrayList;

import build.buildfarm.common.Queue;
import build.buildfarm.common.Visitor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;
import lombok.Getter;
import redis.clients.jedis.AbstractPipeline;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;
import redis.clients.jedis.resps.Tuple;

/**
 * @class RedisPriorityQueue
 * @brief A redis priority queue.
 * @details A redis queue is an implementation of a queue data structure which internally uses redis
 *     to store and distribute the data. Its important to know that the lifetime of the queue
 *     persists before and after the queue data structure is created (since it exists in redis).
 *     Therefore, two redis queues with the same name, would in fact be the same underlying redis
 *     queue.
 */
public class RedisPriorityQueue implements Queue<String> {
  private static final Clock defaultClock = Clock.systemUTC();
  private static final long defaultPollIntervalMillis = 100;

  public static Queue<String> decorate(Jedis jedis, String name) {
    return new RedisPriorityQueue(jedis, name);
  }

  private final Jedis jedis;

  /**
   * @field name
   * @brief The unique name of the queue.
   * @details The name is used by the redis cluster client to access the queue data. If two queues
   *     had the same name, they would be instances of the same underlying redis queue.
   * @return The name of the queue.
   */
  @Getter private final String name;

  private final String script;
  private final Clock clock;
  private final long pollIntervalMillis;

  /**
   * @brief Constructor.
   * @details Construct a named redis queue with an established redis cluster.
   * @param name The global name of the queue.
   */
  public RedisPriorityQueue(Jedis jedis, String name) {
    this(jedis, name, defaultPollIntervalMillis);
  }

  /**
   * @brief Constructor.
   * @details Construct a named redis queue with an established redis cluster. Used to ease the
   *     testing of the order of the queued actions
   * @param name The global name of the queue.
   */
  public RedisPriorityQueue(Jedis jedis, String name, Clock clock) {
    this(jedis, name, clock, defaultPollIntervalMillis);
  }

  /**
   * @brief Constructor.
   * @details Construct a named redis queue with an established redis cluster. Used to ease the
   *     testing of the order of the queued actions
   * @param name The global name of the queue.
   * @param pollIntervalMillis pollInterval to use when dqueuing from redis.
   */
  public RedisPriorityQueue(Jedis jedis, String name, long pollIntervalMillis) {
    this(jedis, name, defaultClock, pollIntervalMillis);
  }

  /**
   * @brief Constructor.
   * @details Construct a named redis queue with an established redis cluster. Used to ease the
   *     testing of the order of the queued actions
   * @param name The global name of the queue.
   * @param time Timestamp of the operation.
   * @param pollIntervalMillis pollInterval to use when dqueuing from redis.
   */
  public RedisPriorityQueue(Jedis jedis, String name, Clock clock, long pollIntervalMillis) {
    this.jedis = jedis;
    this.name = name;
    this.clock = clock;
    this.script = getLuaScript();
    this.pollIntervalMillis = pollIntervalMillis;
  }

  /**
   * @brief Push a value onto the queue with default priority of 0.
   * @details Adds the value into the backend ordered set.
   * @param val The value to push onto the priority queue.
   */
  @Override
  public boolean offer(String val) {
    return offer(val, 0);
  }

  /**
   * @brief Push a value onto the queue with specified priority.
   * @details Adds the value into the backend redis ordered set, with timestamp primary insertion to
   *     guarantee FIFO within a single priority level
   * @param val The value to push onto the priority queue.
   * @param priority The priority of action 0 means highest
   */
  @Override
  public boolean offer(String val, double priority) {
    jedis.zadd(name, priority, clock.millis() + ":" + val);
    return true;
  }

  /**
   * @brief Remove element from dequeue.
   * @details Removes an element from the dequeue and specifies whether it was removed.
   * @param val The value to remove.
   * @return Whether or not the value was removed.
   * @note Suggested return identifier: wasRemoved.
   */
  @Override
  public boolean removeFromDequeue(String val) {
    return jedis.lrem(getDequeueName(), -1, val) != 0;
  }

  @Override
  public void removeFromDequeue(AbstractPipeline pipeline, String val) {
    pipeline.lrem(getDequeueName(), -1, val);
  }

  /**
   * @brief Remove all elements that match from queue.
   * @details Removes all matching elements from the queue and specifies whether it was removed.
   * @param val The value to remove.
   * @return Whether or not the value was removed.
   * @note Suggested return identifier: wasRemoved.
   */
  public boolean removeAll(String val) {
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
  public String take(Duration timeout) throws InterruptedException {
    int maxAttempts = Math.max(1, (int) (timeout.toMillis() / pollIntervalMillis));
    List<String> args = ImmutableList.of(name, getDequeueName(), "true");
    String val;
    for (int i = 0; i < maxAttempts; ++i) {
      Object obj_val = jedis.eval(script, ImmutableList.of(name), args);
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
  public String poll() {
    List<String> args = ImmutableList.of(name, getDequeueName());
    Object obj_val = jedis.eval(script, ImmutableList.of(name), args);
    String val = String.valueOf(obj_val);
    if (!isEmpty(val)) {
      return val;
    }
    return null;
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
    return jedis.zcard(name);
  }

  public Supplier<Long> size(AbstractPipeline pipeline) {
    return pipeline.zcard(name);
  }

  /**
   * @brief Visit each element in the queue.
   * @details Enacts a visitor over each element in the queue.
   * @param visitor A visitor for each visited element in the queue.
   * @note Overloaded.
   */
  @Override
  public void visit(Visitor<String> visitor) {
    visit(name, visitor);
  }

  /**
   * @brief Visit each element in the dequeue.
   * @details Enacts a visitor over each element in the dequeue.
   * @param visitor A visitor for each visited element in the queue.
   */
  @Override
  public void visitDequeue(Visitor<String> visitor) {
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
  private void visit(String queueName, Visitor<String> visitor) {
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

  /** responses from zscan are score:value */
  private String entryValue(String entry) {
    int sepIndex = entry.indexOf(':');
    return entry.substring(sepIndex + 1);
  }

  public ScanResult<String> scan(String queueCursor, int count, String match) {
    // redis has much worse performance when scanning for small counts
    // break even is around 5k
    int scanCount = 5000;
    // maybe use type regular?
    // TODO might be some optimization around unspecified match, look into this
    ScanParams scanParams = new ScanParams().count(scanCount).match(match);
    return new OffsetScanner<String>() {
      @Override
      protected ScanResult<String> scan(String cursor, int remaining) {
        // do we strip here or in transform??
        ScanResult<Tuple> scanResult = jedis.zscan(name, cursor, scanParams);
        return new ScanResult<>(
            scanResult.getCursor(),
            newArrayList(
                Iterables.transform(
                    scanResult.getResult(), entry -> entryValue(entry.getElement()))));
      }
    }.fill(queueCursor, count);
  }
}
