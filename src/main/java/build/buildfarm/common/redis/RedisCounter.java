// Copyright 2021 The Bazel Authors. All rights reserved.
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

import redis.clients.jedis.JedisCluster;

/**
 * @class RedisCounter
 * @brief An atomic counter backed by redis.
 * @details A redis counter is a base-10 64 bit signed integer that can be incremented and
 *     decremented atomically. Its important to know that the lifetime of the value persists before
 *     and after the data structure is created (since it exists in redis). Therefore, two redis
 *     counters with the same name, would in fact be the same underlying count. See more information
 *     about the counter pattern: https://redis.io/commands/incr#pattern-counter.
 */
public class RedisCounter {

  /**
   * @field name
   * @brief The unique name of the counter.
   * @details The name is used by the redis cluster client to access the value. If two counters had
   *     the same name, they would be instances of the same underlying value.
   */
  private String name;

  /**
   * @brief Constructor.
   * @details Construct a redis counter with a name.
   * @param name The global name used by the counter.
   */
  public RedisCounter(String name) {
    this.name = name;
  }
  /**
   * @brief Increment the counter.
   * @details An atomic increment.
   * @param jedis Jedis cluster client.
   */
  public void increment(JedisCluster jedis) {
    jedis.incr(name);
  }
  /**
   * @brief Decrement the counter.
   * @details An atomic decrement.
   * @param jedis Jedis cluster client.
   */
  public void decrement(JedisCluster jedis) {
    jedis.decr(name);
  }
  /**
   * @brief Get current value.
   * @details The value of the counter.
   * @param jedis Jedis cluster client.
   * @return The current value of the counter.
   * @note Suggested return identifier: count.
   */
  public long count(JedisCluster jedis) {
    String value = jedis.get(name);
    if (value != null) {
      return Long.parseLong(value);
    }
    return 0;
  }
}
