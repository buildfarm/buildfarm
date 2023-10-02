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

import build.buildfarm.common.distributed.DistributedMap;
import java.util.AbstractMap;
import java.util.ArrayList;
import com.google.common.collect.Iterables;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.StreamSupport;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisClusterPipeline;
import redis.clients.jedis.Response;

/**
 * @class RedisHashMap
 * @brief A redis hashmap.
 * @details A redis hashmap is an implementation of a map data structure which internally uses redis
 *     to store and distribute the data. Its important to know that the lifetime of the map persists
 *     before and after the map data structure is created (since it exists in redis). Therefore, two
 *     redis maps with the same name, would in fact be the same underlying redis map.
 */
public class RedisHashMap implements Map<String, String> {
  /**
   * @field name
   * @brief The unique name of the map.
   * @details The name is used by the redis cluster client to access the map data. If two maps had
   *     the same name, they would be instances of the same underlying redis map.
   */
  private final String name;

  /**
   * @field jedis
   * @brief The jedis cluster used by the container.
   */
  private final JedisCluster jedis;

  /**
   * @brief Constructor.
   * @details Construct a named redis map with an established redis cluster.
   * @param jedis The jedis cluster used by the container.
   * @param name The global name of the map.
   */
  public RedisHashMap(JedisCluster jedis, String name) {
    this.jedis = jedis;
    this.name = name;
  }

  /**
   * @brief Set key to hold the string value and set key to timeout after a given number of seconds.
   * @details If the key already exists, then the value is replaced.
   * @param key The name of the key.
   * @param value The value for the key.
   * @param expiration_s Timeout to expire the entry. (units: seconds (s))
   * @return Whether a new key was inserted. If a key is overwritten with a new value, this would be
   *     false.
   * @note Overloaded.
   */
  @Override
  public boolean insert(String key, String value, int expiration_s) {
    return jedis.hset(name, key, value) == 1;
  }

  /**
   * @brief Set key to hold the string value. No TTL is available since implementation is redis
   *     hset.
   * @details If the key already exists, then the value is replaced.
   * @param key The name of the key.
   * @param value The value for the key.
   * @return Whether a new key was inserted. If a key is overwritten with a new value, this would be
   *     false.
   */
  @Override
  public boolean insert(String key, String value) {
    return jedis.hset(name, key, value) == 1;
  }

  /**
   * @brief Add key/value only if key doesn't exist.
   * @details If the key already exists, this operation has no effect.
   * @param key The name of the key.
   * @param value The value for the key.
   * @return Whether a new key was inserted. If a key already exists, this would be false.
   */
  @Override
  public boolean insertIfMissing(String key, String value) {
    return jedis.hsetnx(name, key, value) == 1;
  }

  /**
   * @brief Remove a key from the map.
   * @details Deletes the key/value pair.
   * @param key The name of the key.
   * @return Whether the key was removed.
   * @note Overloaded.
   * @note Suggested return identifier: success.
   */
  @Override
  public boolean remove(String key) {
    return jedis.hdel(name, key) == 1;
  }

  /**
   * @brief Remove all given keys from the map.
   * @details Deletes the key/value pairs.
   * @param key The names of the keys.
   */
  @Override
  public void remove(Iterable<String> keys) {
    JedisClusterPipeline p = jedis.pipelined();
    for (String key : keys) {
      p.hdel(name, key);
    }
    p.sync();
  }

  /**
   * @brief Checks whether key exists
   * @details True if key exists. False if it does not.
   * @param key The name of the key.
   * @return Whether the key exists or not in the map.
   */
  @Override
  public boolean exists(String key) {
    return jedis.hexists(name, key);
  }

  /**
   * @brief Get the size of the map.
   * @details O(1).
   * @return The size of the map.
   * @note Suggested return identifier: size.
   */
  @Override
  public int size(JedisCluster jedis) {
    return jedis.hlen(name).intValue();
  }

  /**
   * @brief Get all of the keys from the hashmap.
   * @details No order guarantee
   * @return The redis hashmap keys represented as a set.
   */
  @Override
  public Set<String> keys(JedisCluster jedis) {
    return jedis.hkeys(name);
  }

  /**
   * @brief Convert the redis hashmap to a java map.
   * @details This would not be efficient if the map is large.
   * @return The redis hashmap represented as a java map.
   */
  @Override
  public Map<String, String> asMap(JedisCluster jedis) {
    return jedis.hgetAll(name);
  }

  /**
   * @brief Get the value of the key.
   * @details If the key does not exist, null is returned.
   * @param key The name of the key.
   * @return The value of the key. null if key does not exist.
   * @note Overloaded.
   * @note Suggested return identifier: value.
   */
  @Override
  public String get(String key) {
    return jedis.hget(name, key);
  }

  /**
   * @brief Get the values of the keys.
   * @details If the key does not exist, null is returned.
   * @param keys The name of the keys.
   * @return The values of the keys. null if key does not exist.
   * @note Overloaded.
   * @note Suggested return identifier: values.
   */
  @Override
  public Iterable<Map.Entry<String, String>> get(Iterable<String> keys) {
    // Fetch items via pipeline
    JedisClusterPipeline p = jedis.pipelined();
    List<Map.Entry<String, Response<String>>> values = new ArrayList<>();
    StreamSupport.stream(keys.spliterator(), false)
        .forEach(key -> values.add(new AbstractMap.SimpleEntry<>(key, p.hget(name, key))));
    p.sync();

    List<Map.Entry<String, String>> resolved = new ArrayList<>();
    for (Map.Entry<String, Response<String>> val : values) {
      resolved.add(new AbstractMap.SimpleEntry<>(val.getKey(), val.getValue().get()));
    }
    return resolved;
  }
  /**
   * @brief Get values associated with the specified fields from the hashmap.
   * @param fields The name of the fields.
   * @return Values associated with the specified fields
   */
  public List<String> mget(Iterable<String> fields) {
    return jedis.hmget(name, Iterables.toArray(fields, String.class));
  }
}
