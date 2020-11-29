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

import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.buildfarm.instance.Instance;
import build.buildfarm.v1test.CompletedOperationMetadata;
import build.buildfarm.v1test.ExecutingOperationMetadata;
import build.buildfarm.v1test.QueuedOperationMetadata;
import com.google.longrunning.Operation;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.rpc.PreconditionFailure;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
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
  /// @param   instance An instance is used to get additional information about the operation.
  /// @param   settings Settings on how to find and filter operations.
  /// @return  Results from searching for operations.
  /// @note    Suggested return identifier: results.
  ///
  public static FindOperationsResults findOperations(
      JedisCluster cluster, Instance instance, FindOperationsSettings settings) {
    FindOperationsResults results = new FindOperationsResults();
    results.operations = new ArrayList<>();

    // Get a Jedis node and run the query on it
    Collection<JedisPool> pools = cluster.getClusterNodes().values();
    if (!pools.isEmpty()) {
      try (Jedis node = ((JedisPool) pools.toArray()[0]).getResource()) {
        findOperationNode(cluster, node, settings, results);
      }
    }

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
      List<String> operationKeys = scanOperations(node, cursor, settings);
      collectOperations(cluster, operationKeys, settings.user, results);

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
  ///
  /// @brief   Collect operations based on settings.
  /// @details Populates results.
  /// @param   cluster       An established redis cluster.
  /// @param   operationKeys Keys to get operations from.
  /// @param   user          The user operations to search for.
  /// @param   results       Accumulating results from finding operations.
  ///
  private static void collectOperations(
      JedisCluster cluster,
      List<String> operationKeys,
      String user,
      FindOperationsResults results) {
    for (String operationKey : operationKeys) {
      Operation operation = operationKeyToOperation(cluster, operationKey);
      results.operations.add(operationKey);
    }
  }
  ///
  /// @brief   Convert an operation key into the actual Operation type.
  /// @details Extracts json from redis and parses it. Null if json was
  ///          invalid.
  /// @param   cluster      An established redis cluster.
  /// @param   operationKey The key to lookup and get back the operation of.
  /// @return  The looked up operation.
  /// @note    Suggested return identifier: operation.
  ///
  private static Operation operationKeyToOperation(JedisCluster cluster, String operationKey) {
    String json = cluster.get(operationKey);
    Operation operation = jsonToOperation(json);
    return operation;
  }
  ///
  /// @brief   Convert string json into operation type.
  /// @details Parses json and returns null if invalid.
  /// @param   json The json to convert to Operation type.
  /// @return  The created operation.
  /// @note    Suggested return identifier: operation.
  ///
  private static Operation jsonToOperation(String json) {
    // create a json parser
    JsonFormat.Parser operationParser =
        JsonFormat.parser()
            .usingTypeRegistry(
                JsonFormat.TypeRegistry.newBuilder()
                    .add(CompletedOperationMetadata.getDescriptor())
                    .add(ExecutingOperationMetadata.getDescriptor())
                    .add(ExecuteOperationMetadata.getDescriptor())
                    .add(QueuedOperationMetadata.getDescriptor())
                    .add(PreconditionFailure.getDescriptor())
                    .build())
            .ignoringUnknownFields();

    if (json == null) {
      return null;
    }
    try {
      Operation.Builder operationBuilder = Operation.newBuilder();
      operationParser.merge(json, operationBuilder);
      return operationBuilder.build();
    } catch (InvalidProtocolBufferException e) {
      // logger.log(Level.SEVERE, "error parsing operation from " + json, e);
      return null;
    }
  }
}
