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
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import redis.clients.jedis.JedisCluster;

/**
 * @class RedisQueue
 * @brief A redis queue.
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

  private Timestamp time;
  private final List<String> keys;

  /**
   * @brief Constructor.
   * @details Construct a named redis queue with an established redis cluster.
   * @param name The global name of the queue.
   */
  public RedisPriorityQueue(String name) {
    this.name = name;
    this.time = new Timestamp();
    this.keys = Arrays.asList(name);
  }

  /**
   * @brief Constructor.
   * @details Construct a named redis queue with an established redis cluster. Used to ease the
   *     testing of the order of the queued actions
   * @param name The global name of the queue.
   * @param time Timestamp of the operation.
   */
  public RedisPriorityQueue(String name, Timestamp time) {
    this.name = name;
    this.time = time;
    this.keys = Arrays.asList(name);
  }

  /**
   * @brief Push a value onto the queue with specified priority.
   * @details Adds the value into the backend redis ordered set.
   * @param val The value to push onto the priority queue.
   */
  @Override
  public void push(JedisCluster jedis, String val, double priority) {
    jedis.zadd(name, priority, time.getNanos() + ":" + val);
  }

  /**
   * @brief Push a value onto the queue with default priority of 1.
   * @details Adds the value into the backend rdered set.
   * @param val The value to push onto the priority queue.
   */
  @Override
  public void push(JedisCluster jedis, String val) {
    push(jedis, val, 1);
  }

  /**
   * @brief Remove element from dequeue.
   * @details Removes an element from the dequeue and specifies whether it was removed.
   * @param val The value to remove.
   * @return Whether or not the value was removed.
   * @note Suggested return identifier: wasRemoved.
   */
  @Override
  public boolean removeFromDequeue(JedisCluster jedis, String val) {
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
  public boolean removeAll(JedisCluster jedis, String val) {
    return jedis.zrem(name, val) != 0;
  }

  /**
   * @brief Pop element into internal dequeue and return value.
   * @details This pops the element from one queue atomically into an internal list called the
   *     dequeue. It will wait until the timeout has expired. Null is returned if the timeout has
   *     expired.
   * @param timeout_s Timeout to wait if there is no item to dequeue. (units: seconds (s))
   * @return The value of the transfered element. null if the thread was interrupted.
   * @note Overloaded.
   * @note Suggested return identifier: val.
   */
  @Override
  public String dequeue(JedisCluster jedis, int timeout_s) throws InterruptedException {
    List<String> args = Arrays.asList(name, getDequeueName(), "true");
    for (int i = 0; i < timeout_s; ++i) {
      Object obj_val = jedis.eval(getLuaScript(jedis, "zpoplpush.lua"), keys, args);
      String val = String.valueOf(obj_val);
      if (val != null && !val.isEmpty() && !val.equals("null")) {
        return val;
      }
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
  public String nonBlockingDequeue(JedisCluster jedis) throws InterruptedException {
    List<String> args = Arrays.asList(name, getDequeueName());
    Object obj_val = jedis.eval(getLuaScript(jedis, "zpoplpush.lua"), keys, args);
    String val = String.valueOf(obj_val);
    if (val != null && !val.isEmpty() && !val.equals("null")) {
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
  public long size(JedisCluster jedis) {
    return jedis.zcard(name);
  }

  /**
   * @brief Visit each element in the queue.
   * @details Enacts a visitor over each element in the queue.
   * @param visitor A visitor for each visited element in the queue.
   * @note Overloaded.
   */
  @Override
  public void visit(JedisCluster jedis, StringVisitor visitor) {
    visit(jedis, name, visitor);
  }

  /**
   * @brief Visit each element in the dequeue.
   * @details Enacts a visitor over each element in the dequeue.
   * @param visitor A visitor for each visited element in the queue.
   */
  @Override
  public void visitDequeue(JedisCluster jedis, StringVisitor visitor) {
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
  private void visit(JedisCluster jedis, String queueName, StringVisitor visitor) {
    int listPageSize = 10000;
    int index = 0;
    int nextIndex = listPageSize;
    Set<String> entries;

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
   * @details Load the custom lua script so we can have zpoplpush.
   */
  private String getLuaScript(JedisCluster jedis, String filename) {
    InputStream luaInputStream = this.getClass().getClassLoader().getResourceAsStream(filename);
    if (luaInputStream == null) {
      throw new IllegalArgumentException(filename + " is not found");
    }
    return new BufferedReader(new InputStreamReader(luaInputStream))
            .lines()
            .collect(Collectors.joining("\n"));
  }
}
