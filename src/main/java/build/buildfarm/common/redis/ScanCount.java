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

import static redis.clients.jedis.params.ScanParams.SCAN_POINTER_START;

import com.google.common.collect.Sets;
import java.util.Set;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

/**
 * @class ScanCount
 * @brief Count element of a scan query.
 * @details Some things are laid out in redis where in order to get their conceptual size we need to
 *     query for wildcard keys and count non-duplicates. This may not scale because we need to
 *     filter non duplicates.
 */
public class ScanCount {
  /**
   * @brief Run a scan and count the results.
   * @details This is intended to get the size of certain conceptual containers made of many
   *     different keys.
   * @param cluster An established redis cluster.
   * @param query The query to perform.
   * @param scanCount The count per scan.
   * @return Total number of query results.
   * @note Suggested return identifier: count.
   */
  public static int get(UnifiedJedis jedis, String query, int scanCount) {
    Set<String> keys = Sets.newHashSet();

    if (jedis instanceof JedisCluster cluster) {
      // JedisCluster only supports SCAN commands with MATCH patterns containing hash-tags.
      // This prevents us from using the cluster's SCAN to traverse all existing keys.
      // That's why we choose to scan each of the jedisNode's individually.
      cluster
          .getClusterNodes()
          .values()
          .forEach(
              pool -> {
                try (UnifiedJedis node = new UnifiedJedis(pool.getResource())) {
                  addKeys(node, keys, query, scanCount);
                }
              });
    } else {
      addKeys(jedis, keys, query, scanCount);
    }

    return keys.size();
  }

  /**
   * @brief Scan all entires on node to get key count.
   * @details Keys are accumulated onto.
   * @param node A node of the cluster.
   * @param keys Keys to accumulate.
   * @param query The query to perform.
   * @param scanCount The count per scan.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static void addKeys(UnifiedJedis node, Set<String> keys, String query, int scanCount) {
    // construct query
    ScanParams params = new ScanParams();
    params.match(query);
    params.count(scanCount);

    // iterate over all entries via scanning
    String cursor = ScanParams.SCAN_POINTER_START;
    ScanResult scanResult;
    do {
      scanResult = node.scan(cursor, params);
      if (scanResult != null) {
        keys.addAll(scanResult.getResult());
        cursor = scanResult.getCursor();
      }

    } while (!cursor.equals(SCAN_POINTER_START));
  }
}
