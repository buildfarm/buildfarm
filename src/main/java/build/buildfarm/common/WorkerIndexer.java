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
/// @class   WorkerIndexer
/// @brief   Handle the reindexing the CAS entries based on a departing
///          worker.
/// @details When workers leave the cluster, the CAS keys must be updated to
///          inform other workers that they can no longer obtain CAS data from
///          the missing worker.
///
public class WorkerIndexer {

  ///
  /// @brief   Handle the reindexing the CAS entries based on a departing
  ///          worker.
  /// @details This is intended to be called by a service endpoint as part of
  ///          gracefully shutting down a worker.
  /// @param   cluster  An established redis cluster.
  /// @param   settings Settings on how to traverse the CAS and which worker to remove.
  /// @return  Results from re-indexing the worker in the CAS.
  /// @note    Suggested return identifier: indexResults.
  ///
  public static CasIndexResults removeWorkerIndexesFromCas(
      JedisCluster cluster, CasIndexSettings settings) {
    CasIndexResults results = new CasIndexResults();

    // JedisCluster only supports SCAN commands with MATCH patterns containing hash-tags.
    // This prevents us from using the cluster's SCAN to traverse all of the CAS.
    // That's why we choose to scan each of the jedisNode's individually.
    cluster.getClusterNodes().values().stream()
        .forEach(
            pool -> {
              try (Jedis node = pool.getResource()) {
                reindexNode(cluster, node, settings, results);
              }
            });

    return results;
  }
  ///
  /// @brief   Scan all CAS entires on existing Jedis node and remove
  ///          particular worker indices.
  /// @details Results are accumulated onto.
  /// @param   cluster  An established redis cluster.
  /// @param   node     A node of the cluster.
  /// @param   settings Settings on how to traverse the CAS and which worker to remove.
  /// @param   results  Accumulating results from performing reindexing.
  ///
  private static void reindexNode(
      JedisCluster cluster, Jedis node, CasIndexSettings settings, CasIndexResults results) {
    // iterate over all CAS entries via scanning
    // and remove worker from the CAS keys.
    String cursor = "0";
    do {
      List<String> casKeys = scanCas(node, cursor, settings);
      removeWorkerFromCasKeys(cluster, casKeys, settings.hostName, results);

    } while (!cursor.equals("0"));
  }
  ///
  /// @brief   Scan the cas to obtain CAS keys.
  /// @details Scanning is done incrementally via a cursor.
  /// @param   node     A node of the cluster.
  /// @param   cursor   Scan cursor.
  /// @param   settings Settings on how to traverse the CAS.
  /// @return  Resulting CAS keys from scanning.
  /// @note    Suggested return identifier: casKeys.
  ///
  private static List<String> scanCas(Jedis node, String cursor, CasIndexSettings settings) {
    // construct CAS query
    ScanParams params = new ScanParams();
    params.match(settings.casQuery);
    params.count(settings.scanAmount);

    // perform scan iteration
    ScanResult scanResult = node.scan(cursor, params);
    if (scanResult != null) {
      cursor = scanResult.getCursor();
      return scanResult.getResult();
    }
    return new ArrayList<>();
  }
  ///
  /// @brief   Delete the worker index from the given cas keys.
  /// @details Accumulates results about the deletion.
  /// @param   cluster    An established redis cluster.
  /// @param   casKeys    Keys to remove worker index from.
  /// @param   workerName Index to remove.
  /// @param   results    Accumulating results from performing reindexing.
  ///
  private static void removeWorkerFromCasKeys(
      JedisCluster cluster, List<String> casKeys, String workerName, CasIndexResults results) {
    for (String casKey : casKeys) {
      results.totalKeys++;
      if (cluster.srem(casKey, workerName) == 1) {
        results.removedHosts++;
      }
      if (cluster.scard(casKey) == 0) {
        results.removedKeys++;
        cluster.del(casKey);
      }
    }
  }
}
