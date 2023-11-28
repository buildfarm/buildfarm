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

import lombok.Getter

public class RedisLuaScript {
  /**
   * @field script
   * @brief The Lua script as a string.
   * @field scriptDigest
   * @brief the SHA1 digest of the Lua script.
   */
  @Getter private final String script;
  @Getter private final String digest;

  /**
   * @brief Constructor.
   * @details Construct a named redis queue with an established redis cluster. Used to ease the
   *     testing of the order of the queued actions
   * @param name The global name of the queue.
   * @param time Timestamp of the operation.
   */
  public RedisLuaScript(String script) {
    digest = computeSHA1(script);
  }

  /**
   * @brief Make a Redis eval call via Jedis.
   * @details This attempts to use a cached version of the eval script before falling back to the
   *     non-cached method.
   * @param args Arguments to the Lua script.
   */
  public Object eval(JedisCluster jedis, List<String> keys, List<String> args) {
    try {
      return jedis.evalsha(digest, keys, args);
    } catch (JedisNoScriptException e) {
    }
    return jedis.eval(script, keys, args);
  }

  /**
   * @brief Compute the SHA1 digest of a string
   * @details We can precompute the SHA1 digest of the Lua script and invoke the script on a Redis
   *     server via its SHA1 digest if the server has cached the script.
   */
  private String computeSHA1(String str) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-1");
      byte[] hashBytes = digest.digest(str.getBytes());
      StringBuilder hexString = new StringBuilder();
      for (byte b : hashBytes) {
        String hex = Integer.toHexString(0xff & b);
        if (hex.length() == 1) hexString.append('0');
        hexString.append(hex);
      }
      return hexString.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("SHA-1 algorithm not found", e);
    }
  }
}
