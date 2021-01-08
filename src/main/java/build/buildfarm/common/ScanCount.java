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

import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

///
/// @class   ScanCount
/// @brief   Count element of a scan query.
/// @details Some things are laid out in redis where in order to get their
///          conceptual size we need to query for wildcard keys and count
///          non-duplicates. This may not scale because we need to filter non
///          duplicates.
///
public class ScanCount {

  ///
  /// @brief   Run a scan and count the results.
  /// @details This is intended to get the size of certain conceptual
  ///          containers made of many different keys.
  /// @param   cluster   An established redis cluster.
  /// @param   query     The query to perform.
  /// @param   scanCount The count per scan.
  /// @return  Total number of query results.
  /// @note    Suggested return identifier: count.
  ///
  public static int get(JedisCluster cluster, String query, int scanCount) {
    Set<String> keys = Sets.newHashSet();

    // JedisCluster only supports SCAN commands with MATCH patterns containing hash-tags.
    // This prevents us from using the cluster's SCAN to traverse all existing keys.
    // That's why we choose to scan each of the jedisNode's individually.
    cluster.getClusterNodes().values().stream()
        .forEach(
            pool -> {
              try (Jedis node = pool.getResource()) {
                addKeys(cluster, node, keys, query, scanCount);
              }
            });

    return keys.size();
  }
  ///
  /// @brief   Scan all entires on node to get key count.
  /// @details Keys are accumulated onto.
  /// @param   cluster   An established redis cluster.
  /// @param   node      A node of the cluster.
  /// @param   keys      Keys to accumulate.
  /// @param   query     The query to perform.
  /// @param   scanCount The count per scan.
  ///
  private static void addKeys(
      JedisCluster cluster, Jedis node, Set<String> keys, String query, int scanCount) {
    // iterate over all entries via scanning
    String cursor = "0";
    do {
      keys.addAll(scan(node, cursor, query, scanCount));

    } while (!cursor.equals("0"));
  }
  ///
  /// @brief   Run scan query to get keys.
  /// @details Scanning is done incrementally via a cursor.
  /// @param   node      A node of the cluster.
  /// @param   cursor    Scan cursor.
  /// @param   query     The query to perform.
  /// @param   scanCount The count per scan.
  /// @return  Resulting keys from scanning.
  /// @note    Suggested return identifier: keys.
  ///
  private static List<String> scan(Jedis node, String cursor, String query, int scanCount) {
    // construct query
    ScanParams params = new ScanParams();
    params.match(query);
    params.count(scanCount);

    // perform scan iteration
    ScanResult scanResult = node.scan(cursor, params);
    if (scanResult != null) {
      cursor = scanResult.getCursor();
      return scanResult.getResult();
    }
    return new ArrayList<>();
  }
}
