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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @class RedisHashtags
 * @brief String utilities when dealing with key names that involve hashtags.
 * @details Simple parsers for extracting out / adding hashtags to redis keys.
 */
public class RedisHashtags {
  /**
   * @brief Append the hashtag value to the base queue name.
   * @details Creates a valid queue name for one of the entire queues.
   * @param name The global name of the queue.
   * @param hashtag A hashtag for an individual internal queue.
   * @return A valid queue name for one of the internal queues.
   * @note Suggested return identifier: queueName.
   */
  public static String hashedName(String name, String hashtag) {
    if (hashtag != null && !hashtag.isEmpty()) return "{" + hashtag + "}" + name;
    return name;
  }

  /**
   * @brief Remove any existing redis hashtag from the key name.
   * @details Creates a valid key name with any existing hashtags removed.
   * @param name The global name of the queue.
   * @return A valid keyname without hashtags.
   * @note Suggested return identifier: queueName.
   */
  public static String unhashedName(String name) {
    return name.replaceAll("\\{.*?\\}", "");
  }

  /**
   * @brief Get the existing hashtag of the name.
   * @details Parses out the first redis hashtag found. If no hashtags are found, an empty string is
   *     returned.
   * @param name The global name of the queue.
   * @return The existing hashtag name found in the string (brackets are removed).
   * @note Suggested return identifier: hashtag.
   */
  public static String existingHash(String name) {
    String regex = "\\{.*?\\}";
    Pattern pattern = Pattern.compile(regex);
    Matcher matcher = pattern.matcher(name);

    // hashtag found
    if (matcher.find()) {
      // extract from matcher
      String hashtag = matcher.group(0);

      // remove brackets
      hashtag = hashtag.substring(1, hashtag.length() - 1);

      return hashtag;
    }

    // hashtag not found
    return "";
  }
}
