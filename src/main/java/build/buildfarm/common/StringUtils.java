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

package build.buildfarm.common;


/**
 * @class StringUtils
 * @brief Utilities for working with Java Strings.
 */
public class StringUtils {
  /**
   * @brief Remove a prefix from a string (if it exists).
   * @details Creates a new modified string.
   * @param str The string to remove prefix on.
   * @param prefix The prefix to look for and remove.
   */
  public static String removePrefix(String str, String prefix) {
    if (str.startsWith(prefix)) {
      return str.substring(prefix.length());
    }
    return str;
  }
}
