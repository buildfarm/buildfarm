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

package build.buildfarm.common;

import java.util.ArrayList;
import java.util.List;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

///
/// @class   OperationsFinder
/// @brief   Finds operations based on search settings.
/// @details Operations can be found based on different search queries
///          depending on the context a caller has or wants to filter on.
///
public class OperationsFinder {

  ///
  /// @brief   Finds operations based on search settings.
  /// @details Operations can be found based on different search queries
  ///          depending on the context a caller has or wants to filter on.
  /// @param   cluster  An established redis cluster.
  /// @param   settings Settings on how to find and filter operations.
  /// @return  Results from searching for operations.
  /// @note    Suggested return identifier: results.
  ///
  public static FindOperationsResults findOperations(
      JedisCluster cluster, FindOperationsSettings settings) {
    FindOperationsResults results = new FindOperationsResults();
    results.operations = new ArrayList<>();

    // JedisCluster only supports SCAN commands with MATCH patterns containing hash-tags.
    // This prevents us from using the cluster's SCAN to traverse all of the CAS.
    // That's why we choose to scan each of the jedisNode's individually.
    cluster.getClusterNodes().values().stream()
        .forEach(
            pool -> {
              try (Jedis node = pool.getResource()) {
                findOperationNode(cluster, node, settings, results);
              }
            });

    return results;
  }
  ///
  /// @brief   Scan all operation entires on existing Jedis node and keep ones
  ///          that meet query requirements.
  /// @details Results are accumulated onto.
  /// @param   cluster  An established redis cluster.
  /// @param   node     A node of the cluster.
  /// @param   settings Settings on what operations to find and keep.
  /// @param   results  Accumulating results from performing a search.
  ///
  private static void findOperationNode(
      JedisCluster cluster,
      Jedis node,
      FindOperationsSettings settings,
      FindOperationsResults results) {
    // iterate over all operation entries via scanning
    String cursor = "0";
    do {
      List<String> casKeys = scanOperations(node, cursor, settings);
      // removeWorkerFromCasKeys(cluster, casKeys, settings.hostName, results);

    } while (!cursor.equals("0"));
  }
  ///
  /// @brief   Scan the operations list to obtain operation keys.
  /// @details Scanning is done incrementally via a cursor.
  /// @param   node     A node of the cluster.
  /// @param   cursor   Scan cursor.
  /// @param   settings Settings on how to traverse the Operations.
  /// @return  Resulting operation keys from scanning.
  /// @note    Suggested return identifier: operationKeys.
  ///
  private static List<String> scanOperations(
      Jedis node, String cursor, FindOperationsSettings settings) {
    // construct query
    ScanParams params = new ScanParams();
    params.match(settings.operationQuery);
    params.count(settings.scanAmount);

    // perform scan iteration
    ScanResult scanResult = node.scan(cursor, params);
    if (scanResult != null) {
      cursor = scanResult.getCursor();
      return scanResult.getResult();
    }
    return new ArrayList<>();
  }
}
