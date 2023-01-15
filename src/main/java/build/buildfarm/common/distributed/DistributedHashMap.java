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

package build.buildfarm.common.distributed;

import java.util.Map;
import java.util.Set;

/**
 * @class DistributedHashMap
 * @brief A distributed hashmap.
 * @details A distributed hashmap is an implementation of a hashmap data structure which can be shared across a distributed system.
 *     For example, implementations of this interface might choose to use redis, hazelcast, postgres, etc.
 */
public interface DistributedHashMap <DistributedClient> {

  /**
   * @brief Set key to hold the string value.
   * @details If the key already exists, then the value is replaced.
   * @param client A client used by the implementation.
   * @param key The name of the key.
   * @param value The value for the key.
   * @return Whether a new key was inserted. If a key is overwritten with a new value, this would be
   *     false.
   */
  public boolean insert(DistributedClient client, String key, String value);

  /**
   * @brief Add key/value only if key doesn't exist.
   * @details If the key already exists, this operation has no effect.
   * @param client A client used by the implementation.
   * @param key The name of the key.
   * @param value The value for the key.
   * @return Whether a new key was inserted. If a key already exists, this would be false.
   */
  public boolean insertIfMissing(DistributedClient client, String key, String value);

  /**
   * @brief Checks whether key exists
   * @details True if key exists. False if it does not.
   * @param client A client used by the implementation.
   * @param key The name of the key.
   * @return Whether the key exists or not in the map.
   */
  public boolean exists(DistributedClient client, String key);

  /**
   * @brief Remove a key from the map.
   * @details Deletes the key/value pair.
   * @param client A client used by the implementation.
   * @param key The name of the key.
   * @return Whether the key was removed.
   */
  public boolean remove(DistributedClient client, String key);

  /**
   * @brief Remove all given keys from the map.
   * @details Deletes the key/value pairs.
   * @param client A client used by the implementation.
   * @param key The names of the keys.
   */
  public void remove(DistributedClient client, Iterable<String> keys);

  /**
   * @brief Get the size of the map.
   * @details O(1).
   * @return The size of the map.
   * @note Suggested return identifier: size.
   */
  public long size(DistributedClient client);

  /**
   * @brief Get all of the keys from the hashmap.
   * @details No order guarantee
   * @param client A client used by the implementation.
   * @return The hashmap keys represented as a set.
   */
  public Set<String> keys(DistributedClient client);

  /**
   * @brief Convert the redis hashmap to a java map.
   * @details This would not be efficient if the map is large.
   * @param client A client used by the implementation.
   * @return The hashmap represented as a java map.
   */
  public Map<String, String> asMap(DistributedClient client);
}
