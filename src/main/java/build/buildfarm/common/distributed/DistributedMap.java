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

package build.buildfarm.common.distributed;

import java.util.Map;

/**
 * @class DistributedMap
 * @brief A distributed map.
 * @details A distributed map is an implementation of a map data structure which can be shared
 *     across a distributed system. For example, implementations of this interface might choose to
 *     use redis, hazelcast, postgres, etc.
 */
public interface DistributedMap<DistributedClient> {

  /**
   * @brief Set key to hold the string value and set key to timeout after a given number of seconds.
   * @details If the key already exists, then the value is replaced.
   * @param client A client used by the implementation.
   * @param key The name of the key.
   * @param value The value for the key.
   * @param timeout_s Timeout to expire the entry. (units: seconds (s))
   * @note Overloaded.
   */
  void insert(DistributedClient client, String key, String value, int timeout_s);

  /**
   * @brief Set key to hold the string value and set key to timeout after a given number of seconds.
   * @details If the key already exists, then the value is replaced.
   * @param client A client used by the implementation.
   * @param key The name of the key.
   * @param value The value for the key.
   * @param timeout_s Timeout to expire the entry. (units: seconds (s))
   * @note Overloaded.
   */
  void insert(DistributedClient client, String key, String value, long timeout_s);

  /**
   * @brief Set key to hold the string value and set key to timeout after a given number of seconds.
   * @details If the key already exists, then the value is replaced.
   * @param client A client used by the implementation.
   * @param key The name of the key.
   * @param value The value for the key.
   * @note Overloaded.
   */
  void insert(DistributedClient client, String key, String value);

  /**
   * @brief Remove a key from the map.
   * @details Deletes the key/value pair.
   * @param client A client used by the implementation.
   * @param key The name of the key.
   * @note Overloaded.
   */
  void remove(DistributedClient client, String key);

  /**
   * @brief Remove multiple keys from the map.
   * @details Done via pipeline.
   * @param client A client used by the implementation.
   * @param keys The name of the keys.
   * @note Overloaded.
   */
  void remove(DistributedClient client, Iterable<String> keys);

  /**
   * @brief Get the value of the key.
   * @details If the key does not exist, null is returned.
   * @param client A client used by the implementation.
   * @param key The name of the key.
   * @return The value of the key. null if key does not exist.
   * @note Overloaded.
   * @note Suggested return identifier: value.
   */
  String get(DistributedClient client, String key);

  /**
   * @brief Get the values of the keys.
   * @details If the key does not exist, null is returned.
   * @param client A client used by the implementation.
   * @param keys The name of the keys.
   * @return The values of the keys. null if key does not exist.
   * @note Overloaded.
   * @note Suggested return identifier: values.
   */
  Iterable<Map.Entry<String, String>> get(DistributedClient client, Iterable<String> keys);

  /**
   * @brief whether the key exists
   * @details True if key exists. False if key does not exist.
   * @param client A client used by the implementation.
   * @param key The name of the key.
   * @return Whether the key exists or not.
   * @note Suggested return identifier: exists.
   */
  boolean exists(DistributedClient client, String key);

  /**
   * @brief Get the size of the map.
   * @details May be inefficient to due scanning into memory and deduplicating.
   * @param client A client used by the implementation.
   * @return The size of the map.
   * @note Suggested return identifier: size.
   */
  int size(DistributedClient client);
}
