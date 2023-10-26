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

import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.buildfarm.instance.Instance;
import build.buildfarm.operations.EnrichedOperation;
import build.buildfarm.operations.FindOperationsResults;
import build.buildfarm.operations.FindOperationsSettings;
import build.buildfarm.v1test.CompletedOperationMetadata;
import build.buildfarm.v1test.ExecutingOperationMetadata;
import build.buildfarm.v1test.QueuedOperationMetadata;
import com.google.longrunning.Operation;
import com.google.protobuf.util.JsonFormat;
import com.google.rpc.PreconditionFailure;
import com.jayway.jsonpath.JsonPath;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

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
  public static FindOperationsResults findEnrichedOperations(
      UnifiedJedis jedis, Instance instance, FindOperationsSettings settings) {
    FindOperationsResults results = new FindOperationsResults();
    results.operations = new HashMap<>();

    adjustFilter(settings);

    if (jedis instanceof JedisCluster) {
      JedisCluster cluster = (JedisCluster) jedis;
      // JedisCluster only supports SCAN commands with MATCH patterns containing hash-tags.
      // This prevents us from using the cluster's SCAN to traverse all of the CAS.
      // That's why we choose to scan each of the jedisNode's individually.
      cluster
          .getClusterNodes()
          .values()
          .forEach(
              pool -> {
                try (UnifiedJedis node = new UnifiedJedis(pool.getResource())) {
                  findEnrichedOperationOnNode(cluster, node, instance, settings, results);
                }
              });
    } else {
      findEnrichedOperationOnNode(jedis, jedis, instance, settings, results);
    }

    return results;
  }

  /**
   * @brief Finds operations based on search settings.
   * @details Operations can be found based on different search queries depending on the context a
   *     caller has or wants to filter on.
   * @param cluster An established redis cluster.
   * @param settings Settings on how to find and filter operations.
   * @return Results from searching for operations.
   * @note Suggested return identifier: results.
   */
  public static List<Operation> findOperations(
      UnifiedJedis jedis, FindOperationsSettings settings) {
    List<Operation> results = new ArrayList<>();

    adjustFilter(settings);

    if (jedis instanceof JedisCluster) {
      JedisCluster cluster = (JedisCluster) jedis;
      // JedisCluster only supports SCAN commands with MATCH patterns containing hash-tags.
      // This prevents us from using the cluster's SCAN to traverse all of the CAS.
      // That's why we choose to scan each of the jedisNode's individually.
      cluster
          .getClusterNodes()
          .values()
          .forEach(
              pool -> {
                try (UnifiedJedis node = new UnifiedJedis(pool.getResource())) {
                  findOperationOnNode(cluster, node, settings, results);
                }
              });
    } else {
      findOperationOnNode(jedis, jedis, settings, results);
    }

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
  private static void findEnrichedOperationOnNode(
      UnifiedJedis cluster,
      UnifiedJedis node,
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
   * @brief Scan all operation entries on existing Jedis node and keep ones that meet query
   *     requirements.
   * @details Results are accumulated onto.
   * @param cluster An established redis cluster.
   * @param node A node of the cluster.
   * @param settings Settings on what operations to find and keep.
   * @param results Accumulating results from performing a search.
   */
  private static void findOperationOnNode(
      UnifiedJedis cluster,
      UnifiedJedis node,
      FindOperationsSettings settings,
      List<Operation> results) {
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
        collectOperations(cluster, scanResult.getResult(), settings.filterPredicate, results);
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
      UnifiedJedis cluster,
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
   * @brief Collect operations based on settings.
   * @details Populates results.
   * @param cluster An established redis cluster.
   * @param operationKeys Keys to get operations from.
   * @param filterPredicate The search query used to find particular operations.
   * @param results Accumulating results from finding operations.
   */
  private static void collectOperations(
      UnifiedJedis cluster,
      List<String> operationKeys,
      String filterPredicate,
      List<Operation> results) {
    for (String operationKey : operationKeys) {
      Operation operation = EnrichedOperationBuilder.operationKeyToOperation(cluster, operationKey);
      if (shouldKeepOperation(operation, filterPredicate)) {
        results.add(operation);
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

  /**
   * @brief Whether or not to keep operation based on filter settings.
   * @details True if the operation should be returned. false if it should be ignored.
   * @param operation The operation to analyze based on filter settings.
   * @param filterPredicate The search query used to find particular operations.
   * @return Whether to keep the operation based on the filter settings.
   * @note Suggested return identifier: shouldKeep.
   */
  private static boolean shouldKeepOperation(Operation operation, String filterPredicate) {
    String json = toJsonString(operation);

    // test the predicate
    try {
      List<Map<String, Object>> matches = JsonPath.parse(json).read(filterPredicate);
      return !matches.isEmpty();
    } catch (Exception e) {
      return false;
    }
  }

  private static String toJsonString(Operation operation) {
    try {
      JsonFormat.Printer operationPrinter =
          JsonFormat.printer()
              .usingTypeRegistry(
                  JsonFormat.TypeRegistry.newBuilder()
                      .add(CompletedOperationMetadata.getDescriptor())
                      .add(ExecutingOperationMetadata.getDescriptor())
                      .add(ExecuteOperationMetadata.getDescriptor())
                      .add(QueuedOperationMetadata.getDescriptor())
                      .add(PreconditionFailure.getDescriptor())
                      .build());

      JSONParser j = new JSONParser();
      JSONObject obj = new JSONObject();
      obj.put("operation", j.parse(operationPrinter.print(operation)));
      return obj.toJSONString();
    } catch (IOException | ParseException e) {
      throw new RuntimeException(e);
    }
  }
}
