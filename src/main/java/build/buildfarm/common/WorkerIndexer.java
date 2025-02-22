// Copyright 2020 The Buildfarm Authors. All rights reserved.
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

import io.prometheus.metrics.core.metrics.Gauge;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.java.Log;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

/**
 * @class WorkerIndexer
 * @brief Handle the reindexing the CAS entries based on a departing worker.
 * @details When workers leave the cluster, the CAS keys must be updated to inform other workers
 *     that they can no longer obtain CAS data from the missing worker.
 */
@Log
public class WorkerIndexer {
  private static final Gauge indexerKeysRemovedGauge =
      Gauge.builder()
          .name("cas_indexer_removed_keys")
          .labelNames("node")
          .help("Indexer results - Number of keys removed")
          .register();
  private static final Gauge indexerHostsRemovedGauge =
      Gauge.builder()
          .name("cas_indexer_removed_hosts")
          .labelNames("node")
          .help("Indexer results - Number of hosts removed")
          .register();

  /**
   * @brief Handle the reindexing the CAS entries based on a departing worker.
   * @details This is intended to be called by a service endpoint as part of gracefully shutting
   *     down a worker.
   * @param cluster An established redis cluster.
   * @param settings Settings on how to traverse the CAS and which worker to remove.
   * @return Results from re-indexing the worker in the CAS.
   * @note Suggested return identifier: indexResults.
   */
  public static CasIndexResults removeWorkerIndexesFromCas(
      UnifiedJedis jedis, CasIndexSettings settings) {
    CasIndexResults results = new CasIndexResults();

    if (jedis instanceof JedisCluster cluster) {
      // JedisCluster only supports SCAN commands with MATCH patterns containing hash-tags.
      // This prevents us from using the cluster's SCAN to traverse all of the CAS.
      // That's why we choose to scan each of the jedisNode's individually.
      cluster
          .getClusterNodes()
          .values()
          .forEach(
              pool -> {
                try (UnifiedJedis node = new UnifiedJedis(pool.getResource())) {
                  reindexNode(cluster, node, settings, results);
                }
              });
    } else {
      reindexNode(jedis, jedis, settings, results);
    }
    return results;
  }

  /**
   * @brief Scan all CAS entires on existing Jedis node and remove particular worker indices.
   * @details Results are accumulated onto.
   * @param cluster An established redis cluster.
   * @param node A node of the cluster.
   * @param settings Settings on how to traverse the CAS and which worker to remove.
   * @param results Accumulating results from performing reindexing.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static void reindexNode(
      UnifiedJedis cluster, UnifiedJedis node, CasIndexSettings settings, CasIndexResults results) {
    Long totalKeys = 0L;
    Long removedKeys = 0L;
    Long removedHosts = 0L;
    Set<String> activeWorkers = cluster.hkeys("Workers");
    log.info(
        String.format(
            "Initializing CAS Indexer for Node %s with %d active workers.",
            node.toString(), activeWorkers.size()));

    // iterate over all CAS entries via scanning
    // and remove worker from the CAS keys.
    // construct CAS query
    ScanParams params = new ScanParams();
    params.match(settings.casQuery);
    params.count(settings.scanAmount);

    String cursor = "0";
    ScanResult scanResult;
    do {
      scanResult = node.scan(cursor, params);
      if (scanResult != null) {
        List<String> casKeys = scanResult.getResult();
        for (String casKey : casKeys) {
          totalKeys += casKeys.size();
          Set<String> intersectSource = cluster.smembers(casKey);
          Set<String> intersectResult =
              intersectSource.stream()
                  .distinct()
                  .filter(activeWorkers::contains)
                  .collect(Collectors.toSet());
          removedHosts += (intersectSource.size() - intersectResult.size());
          if (intersectResult.isEmpty()) {
            removedKeys++;
            cluster.del(casKey);
          } else {
            cluster.sadd(casKey, intersectResult.toArray(new String[0]));
          }
        }
        cursor = scanResult.getCursor();
      }
    } while (!cursor.equals("0"));
    results.totalKeys += totalKeys;
    results.removedKeys += removedKeys;
    results.removedHosts += removedHosts;
    indexerHostsRemovedGauge.labelValues(node.toString()).set(removedHosts);
    indexerKeysRemovedGauge.labelValues(node.toString()).set(removedKeys);
  }
}
