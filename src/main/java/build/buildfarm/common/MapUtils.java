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

package build.buildfarm.common;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * @class MapUtils
 * @brief Utilities for working with Java maps.
 */
public class MapUtils {
  /**
   * @brief Increment the value of any key.
   * @details Add the key with value 1 if it does not previously exist.
   * @param map The map to find and increment the key in.
   * @param key The key to increment the value of.
   */
  public static <K> void incrementValue(Map<K, Integer> map, K key) {
    Integer count = map.get(key);
    if (count == null) {
      map.put(key, 1);
    } else {
      map.put(key, count + 1);
    }
  }

  /**
   * @brief Convert map to printable string.
   * @details Uses streams.
   * @param map Map to convert to string.
   * @return String representation of map.
   * @note Suggested return identifier: str.
   */
  public static String toString(Map<?, ?> map) {
    return map.keySet().stream()
        .map(key -> key + "=" + map.get(key))
        .collect(Collectors.joining(", ", "{", "}"));
  }
}
