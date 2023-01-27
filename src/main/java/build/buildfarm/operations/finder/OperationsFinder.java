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

package build.buildfarm.operations.finder;

import build.buildfarm.instance.Instance;
import build.buildfarm.operations.EnrichedOperation;
import build.buildfarm.operations.FindOperationsResults;
import build.buildfarm.operations.FindOperationsSettings;
import com.jayway.jsonpath.JsonPath;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

/**
 * @class OperationsFinder
 * @brief Finds operations based on search settings.
 * @details Operations can be found based on different search queries depending on the context a
 *     caller has or wants to filter on.
 */
public class OperationsFinder {
  /**
   * @brief Finds operations based on search settings.
   * @details Operations can be found based on different search queries depending on the context a
   *     caller has or wants to filter on.
   * @param cluster An established redis cluster.
   * @param instance An instance is used to get additional information about the operation.
   * @param settings Settings on how to find and filter operations.
   * @return Results from searching for operations.
   * @note Suggested return identifier: results.
   */
  public static FindOperationsResults findOperations(
      JedisCluster cluster, Instance instance, FindOperationsSettings settings) {
    FindOperationsResults results = new FindOperationsResults();
    results.operations = new HashMap<>();

    adjustFilter(settings);

    // JedisCluster only supports SCAN commands with MATCH patterns containing hash-tags.
    // This prevents us from using the cluster's SCAN to traverse all of the CAS.
    // That's why we choose to scan each of the jedisNode's individually.
    cluster
        .getClusterNodes()
        .values()
        .forEach(
            pool -> {
              try (Jedis node = pool.getResource()) {
                findOperationNode(cluster, node, instance, settings, results);
              }
            });

    return results;
  }

  /**
   * @brief Adjust the user provided filter if needed.
   * @details This is used to ensure certain expectations on particular given filters.
   * @param settings Settings on how to find and filter operations.
   */
  private static void adjustFilter(FindOperationsSettings settings) {
    // be generous. if no filter is provided assume the user wants to select all operations instead
    // of no operations
    if (settings.filterPredicate.isEmpty()) {
      settings.filterPredicate = "*";
    }
  }

  /**
   * @brief Scan all operation entries on existing Jedis node and keep ones that meet query
   *     requirements.
   * @details Results are accumulated onto.
   * @param cluster An established redis cluster.
   * @param node A node of the cluster.
   * @param instance An instance is used to get additional information about the operation.
   * @param settings Settings on what operations to find and keep.
   * @param results Accumulating results from performing a search.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static void findOperationNode(
      JedisCluster cluster,
      Jedis node,
      Instance instance,
      FindOperationsSettings settings,
      FindOperationsResults results) {
    // iterate over all operation entries via scanning

    // construct query
    ScanParams params = new ScanParams();
    params.match(settings.operationQuery);
    params.count(settings.scanAmount);

    String cursor = "0";
    ScanResult scanResult;
    do {
      scanResult = node.scan(cursor, params);
      if (scanResult != null) {
        cursor = scanResult.getCursor();
        collectOperations(
            cluster, instance, scanResult.getResult(), settings.filterPredicate, results);
      }
    } while (!cursor.equals("0"));
  }

  /**
   * @brief Collect operations based on settings.
   * @details Populates results.
   * @param cluster An established redis cluster.
   * @param instance An instance is used to get additional information about the operation.
   * @param operationKeys Keys to get operations from.
   * @param filterPredicate The search query used to find particular operations.
   * @param results Accumulating results from finding operations.
   */
  private static void collectOperations(
      JedisCluster cluster,
      Instance instance,
      List<String> operationKeys,
      String filterPredicate,
      FindOperationsResults results) {
    for (String operationKey : operationKeys) {
      EnrichedOperation operation = EnrichedOperationBuilder.build(cluster, instance, operationKey);
      if (shouldKeepOperation(operation, filterPredicate)) {
        results.operations.put(operationKey, operation);
      }
    }
  }

  /**
   * @brief Whether or not to keep operation based on filter settings.
   * @details True if the operation should be returned. false if it should be ignored.
   * @param operation The operation to analyze based on filter settings.
   * @param filterPredicate The search query used to find particular operations.
   * @return Whether to keep the operation based on the filter settings.
   * @note Suggested return identifier: shouldKeep.
   */
  private static boolean shouldKeepOperation(EnrichedOperation operation, String filterPredicate) {
    String json = operation.asJsonString();

    // test the predicate
    try {
      List<Map<String, Object>> matches = JsonPath.parse(json).read(filterPredicate);
      return !matches.isEmpty();
    } catch (Exception e) {
      return false;
    }
  }
}
