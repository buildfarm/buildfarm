// Copyright 2021 The Buildfarm Authors. All rights reserved.
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

import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;
import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;

/**
 * @class RedissonMap
 * @brief A redis map.
 * @details A redis map is an implementation of a map data structure which internally uses redisson
 *     to store and distribute the data. Its important to know that the lifetime of the map persists
 *     before and after the map data structure is created (since it exists in redis). Therefore, two
 *     redis maps with the same name, would in fact be the same underlying redis map. This provides
 *     a simple interface for String-to-String maps. You may want to use the Redisson containers
 *     directly instead of through this limited API. This container exists for transitional purposes
 *     between jedis and redisson.
 */
public class RedissonMap {
  /**
   * @field cacheMap
   * @brief A memory cached redis container.
   * @details The redisson container is wrapped
   */
  private RMapCache<String, String> cacheMap;

  /**
   * @brief Constructor.
   * @details Construct a named redis map.
   * @param client The redisson client used to initialize the cache container.
   * @param name The global name of the map.
   */
  public RedissonMap(RedissonClient client, String name) {
    this.cacheMap = client.getMapCache(name);
  }

  /**
   * @brief Set key to hold the string value and set key to timeout after a given number of seconds.
   * @details If the key already exists, then the value is replaced.
   * @param key The name of the key.
   * @param value The value for the key.
   * @param timeout_s Timeout to expire the entry. (units: seconds (s))
   */
  public void insert(String key, String value, int timeout_s) {
    cacheMap.put(key, value, timeout_s, TimeUnit.SECONDS);
  }

  /**
   * @brief Remove a key from the map.
   * @details Deletes the key/value pair.
   * @param key The name of the key.
   * @note Overloaded.
   */
  public void remove(String key) {
    cacheMap.remove(key);
  }

  /**
   * @brief Remove multiple keys from the map.
   * @details Done via pipeline.
   * @param keys The name of the keys.
   * @note Overloaded.
   */
  public void remove(Iterable<String> keys) {
    cacheMap.fastRemove(StreamSupport.stream(keys.spliterator(), false).toArray(String[]::new));
  }

  /**
   * @brief Get the value of the key.
   * @details If the key does not exist, null is returned.
   * @param key The name of the key.
   * @return The value of the key. null if key does not exist.
   * @note Suggested return identifier: value.
   */
  public String get(String key) {
    return cacheMap.get(key);
  }

  /**
   * @brief whether the key exists
   * @details True if key exists. False if key does not exist.
   * @param key The name of the key.
   * @return Whether the key exists or not.
   * @note Suggested return identifier: exists.
   */
  public boolean exists(String key) {
    return get(key) != null;
  }

  /**
   * @brief Get the size of the map.
   * @details O(1).
   * @return The size of the map.
   * @note Suggested return identifier: size.
   */
  public int size() {
    return cacheMap.size();
  }
}
