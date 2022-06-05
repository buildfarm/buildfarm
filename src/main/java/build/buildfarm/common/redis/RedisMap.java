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

import build.buildfarm.common.ScanCount;
import java.util.List;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisClusterPipeline;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

/**
 * @class RedisMap
 * @brief A redis map.
 * @details A redis map is an implementation of a map data structure which internally uses redis to
 *     store and distribute the data. Its important to know that the lifetime of the map persists
 *     before and after the map data structure is created (since it exists in redis). Therefore, two
 *     redis maps with the same name, would in fact be the same underlying redis map.
 */
public class RedisMap {
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
  public RedisMap(String name) {
    this.name = name;
  }

  /**
   * @brief Set key to hold the string value and set key to timeout after a given number of seconds.
   * @details If the key already exists, then the value is replaced.
   * @param jedis Jedis cluster client.
   * @param key The name of the key.
   * @param value The value for the key.
   * @param timeout_s Timeout to expire the entry. (units: seconds (s))
   * @note Overloaded.
   */
  public void insert(JedisCluster jedis, String key, String value, int timeout_s) {
    jedis.setex(createKeyName(key), timeout_s, value);
  }

  /**
   * @brief Set key to hold the string value and set key to timeout after a given number of seconds.
   * @details If the key already exists, then the value is replaced.
   * @param jedis Jedis cluster client.
   * @param key The name of the key.
   * @param value The value for the key.
   * @param timeout_s Timeout to expire the entry. (units: seconds (s))
   * @note Overloaded.
   */
  public void insert(JedisCluster jedis, String key, String value, long timeout_s) {
    // Jedis only provides int precision.  this is fine as the units are seconds.
    // We supply an interface for longs as a convenience to callers.
    jedis.setex(createKeyName(key), (int) timeout_s, value);
  }

  /**
   * @brief Remove a key from the map.
   * @details Deletes the key/value pair.
   * @param jedis Jedis cluster client.
   * @param key The name of the key.
   * @note Overloaded.
   */
  public void remove(JedisCluster jedis, String key) {
    jedis.del(createKeyName(key));
  }

  /**
   * @brief Remove multiple keys from the map.
   * @details Done via pipeline.
   * @param jedis Jedis cluster client.
   * @param keys The name of the keys.
   * @note Overloaded.
   */
  public void remove(JedisCluster jedis, Iterable<String> keys) {
    JedisClusterPipeline p = jedis.pipelined();
    for (String key : keys) {
      p.del(createKeyName(key));
    }
    p.sync();
  }

  /**
   * @brief Remove all the elements from the map.
   * @details Deletes all the key/value pairs.
   * @param jedis Jedis cluster client.
   */
  public void clear(JedisCluster jedis) {
    jedis
        .getClusterNodes()
        .values()
        .forEach(
            pool -> {
              try (Jedis node = pool.getResource()) {
                // construct query
                ScanParams params = new ScanParams();
                params.match(allKeyNames());
                params.count(1000);

                // iterate over all entries via scanning
                String cursor = "0";
                ScanResult scanResult;
                do {
                  scanResult = node.scan(cursor, params);
                  if (scanResult != null) {
                    List<String> keyResults = scanResult.getResult();
                    for (String key : keyResults) {
                      jedis.del(key);
                    }
                    cursor = scanResult.getCursor();
                  }

                } while (!cursor.equals("0"));
              }
            });
  }

  /**
   * @brief Get the value of the key.
   * @details If the key does not exist, null is returned.
   * @param jedis Jedis cluster client.
   * @param key The name of the key.
   * @return The value of the key. null if key does not exist.
   * @note Suggested return identifier: value.
   */
  public String get(JedisCluster jedis, String key) {
    return jedis.get(createKeyName(key));
  }

  /**
   * @brief whether the key exists
   * @details True if key exists. False if key does not exist.
   * @param jedis Jedis cluster client.
   * @param key The name of the key.
   * @return Whether the key exists or not.
   * @note Suggested return identifier: exists.
   */
  public boolean exists(JedisCluster jedis, String key) {
    return jedis.exists(createKeyName(key));
  }

  /**
   * @brief Get the size of the map.
   * @details May be inefficient to due scanning into memory and deduplicating.
   * @param jedis Jedis cluster client.
   * @return The size of the map.
   * @note Suggested return identifier: size.
   */
  public int size(JedisCluster jedis) {
    return ScanCount.get(jedis, name + ":*", 1000);
  }

  /**
   * @brief Create the key name used in redis.
   * @details The key name is made more unique by leveraging the map's name.
   * @param keyName The name of the key.
   * @return The key name to use in redis.
   * @note Suggested return identifier: redisKeyName.
   */
  private String createKeyName(String keyName) {
    return name + ":" + keyName;
  }

  /**
   * @brief The wildcard expression to all map key names.
   * @details Used for running commands on all of the keys. For example: deleting.
   * @return The expression to match with all key names.
   * @note Suggested return identifier: allKeyNames.
   */
  private String allKeyNames() {
    return name + ":*";
  }
}
