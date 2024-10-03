// Copyright 2022 The Bazel Authors. All rights reserved.
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

import com.google.common.collect.Iterables;
import java.util.List;
import java.util.Map;
import java.util.Set;
import redis.clients.jedis.AbstractPipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

/**
 * @class RedisHashMap
 * @brief A redis hashmap.
 * @details A redis hashmap is an implementation of a map data structure which internally uses redis
 *     to store and distribute the data. Its important to know that the lifetime of the map persists
 *     before and after the map data structure is created (since it exists in redis). Therefore, two
 *     redis maps with the same name, would in fact be the same underlying redis map.
 */
public class RedisHashMap {
  /**
   * @field name
   * @brief The unique name of the map.
   * @details The name is used by the redis cluster client to access the map data. If two maps had
   *     the same name, they would be instances of the same underlying redis map.
   */
  private final String name;

  /**
   * @brief Constructor.
   * @details Construct a named redis map with an established redis cluster.
   * @param name The global name of the map.
   */
  public RedisHashMap(String name) {
    this.name = name;
  }

  /**
   * @brief Set key to hold the string value. No TTL is available since implementation is redis
   *     hset.
   * @details If the key already exists, then the value is replaced.
   * @param jedis Jedis cluster client.
   * @param key The name of the key.
   * @param value The value for the key.
   * @return Whether a new key was inserted. If a key is overwritten with a new value, this would be
   *     false.
   */
  public boolean insert(UnifiedJedis jedis, String key, String value) {
    return jedis.hset(name, key, value) == 1;
  }

  /**
   * @brief Add key/value only if key doesn't exist.
   * @details If the key already exists, this operation has no effect.
   * @param jedis Jedis cluster client.
   * @param key The name of the key.
   * @param value The value for the key.
   * @return Whether a new key was inserted. If a key already exists, this would be false.
   */
  public boolean insertIfMissing(UnifiedJedis jedis, String key, String value) {
    return jedis.hsetnx(name, key, value) == 1;
  }

  /**
   * @brief Checks whether key exists
   * @details True if key exists. False if it does not.
   * @param jedis Jedis cluster client.
   * @param key The name of the key.
   * @return Whether the key exists or not in the map.
   */
  public boolean exists(UnifiedJedis jedis, String key) {
    return jedis.hexists(name, key);
  }

  /**
   * @brief Remove a key from the map.
   * @details Deletes the key/value pair.
   * @param jedis Jedis cluster client.
   * @param key The name of the key.
   * @return Whether the key was removed.
   */
  public boolean remove(UnifiedJedis jedis, String key) {
    return jedis.hdel(name, key) == 1;
  }

  /**
   * @brief Remove all given keys from the map.
   * @details Deletes the key/value pairs.
   * @param jedis Jedis cluster client.
   * @param key The names of the keys.
   */
  public void remove(UnifiedJedis jedis, Iterable<String> keys) {
    try (AbstractPipeline p = jedis.pipelined()) {
      for (String key : keys) {
        p.hdel(name, key);
      }
    }
  }

  /**
   * @brief Get the size of the map.
   * @details O(1).
   * @return The size of the map.
   * @note Suggested return identifier: size.
   */
  public long size(UnifiedJedis jedis) {
    return jedis.hlen(name);
  }

  public Response<Long> size(AbstractPipeline pipeline) {
    return pipeline.hlen(name);
  }

  /**
   * @brief Get all of the keys from the hashmap.
   * @details No order guarantee
   * @param jedis Jedis cluster client.
   * @return The redis hashmap keys represented as a set.
   */
  public Set<String> keys(UnifiedJedis jedis) {
    return jedis.hkeys(name);
  }

  public ScanResult<Map.Entry<String, String>> scan(
      UnifiedJedis jedis, String hashCursor, int count) {
    // unlike full map search, we should have good scan key coherency
    // avoid switching this around while trying to pad out the results
    int scanCount = count * 2;
    ScanParams scanParams = new ScanParams().count(scanCount);
    return new OffsetScanner<Map.Entry<String, String>>() {
      @Override
      protected ScanResult<Map.Entry<String, String>> scan(String cursor, int remaining) {
        return jedis.hscan(name, cursor, scanParams);
      }
    }.fill(hashCursor, count);
  }

  /**
   * @brief Convert the redis hashmap to a java map.
   * @details This would not be efficient if the map is large.
   * @param jedis Jedis cluster client.
   * @return The redis hashmap represented as a java map.
   */
  public Map<String, String> asMap(UnifiedJedis jedis) {
    return jedis.hgetAll(name);
  }

  /**
   * @brief Get values associated with the specified fields from the hashmap.
   * @param jedis Jedis cluster client.
   * @param fields The name of the fields.
   * @return Values associated with the specified fields
   */
  public List<String> mget(UnifiedJedis jedis, Iterable<String> fields) {
    return jedis.hmget(name, Iterables.toArray(fields, String.class));
  }
}
