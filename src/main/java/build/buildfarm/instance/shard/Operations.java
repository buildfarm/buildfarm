// Copyright 2023 The Bazel Authors. All rights reserved.
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

package build.buildfarm.instance.shard;

import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Lists.newArrayList;

import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.buildfarm.common.redis.RedisMap;
import build.buildfarm.common.redis.RedisSetMap;
import build.buildfarm.v1test.QueuedOperationMetadata;
import com.google.longrunning.Operation;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.rpc.PreconditionFailure;
import java.util.logging.Level;
import lombok.extern.java.Log;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.resps.ScanResult;

/**
 * @class Operations
 * @brief Stores all operations that have taken place on the system.
 * @details We keep track of them in the distributed state to avoid them getting lost if a
 *     particular machine goes down. They should also exist for some period of time after a build
 *     invocation has finished so that developers can lookup the status of their build and
 *     information about the operations that ran.
 */
@Log
public class Operations {
  private static final JsonFormat.Parser operationParser =
      JsonFormat.parser()
          .usingTypeRegistry(
              JsonFormat.TypeRegistry.newBuilder()
                  .add(ExecuteOperationMetadata.getDescriptor())
                  .add(QueuedOperationMetadata.getDescriptor())
                  .add(PreconditionFailure.getDescriptor())
                  .build())
          .ignoringUnknownFields();

  static JsonFormat.Parser getParser() {
    return operationParser;
  }

  public RedisSetMap toolInvocations;

  /**
   * @field operations
   * @brief A mapping from operationName -> operation
   * @details Operation names are unique.
   */
  public RedisMap operations;

  /**
   * @brief Constructor.
   * @details Construct container for operations.
   * @param name The global name of the operation map.
   * @param timeout_s When to expire operations.
   */
  public Operations(RedisSetMap toolInvocations, String name, int timeout_s) {
    this.toolInvocations = toolInvocations;
    operations = new RedisMap(name, timeout_s);
  }

  /**
   * @brief Get the operation by operationID.
   * @details If the operation does not exist, null is returned.
   * @param jedis Jedis cluster client.
   * @param operationId The ID of the operation.
   * @return The json of the operation. null if key does not exist.
   * @note Overloaded.
   * @note Suggested return identifier: operation.
   */
  public Operation get(UnifiedJedis jedis, String name) {
    return parse(operations.get(jedis, name));
  }

  /**
   * @brief Get the operations by operationIDs.
   * @details If the operation does not exist, null is returned.
   * @param jedis Jedis cluster client.
   * @param names The names of the operations.
   * @return The json of the operations. null if the operation does not exist.
   * @note Overloaded.
   * @note Suggested return identifier: operations.
   */
  public Iterable<Operation> get(UnifiedJedis jedis, Iterable<String> names) {
    return transform(operations.get(jedis, names), entry -> Operations.parse(entry.getValue()));
  }

  private static Operation parse(String operationJson) {
    if (operationJson != null) {
      try {
        Operation.Builder operationBuilder = Operation.newBuilder();
        operationParser.merge(operationJson, operationBuilder);
        return operationBuilder.build();
      } catch (InvalidProtocolBufferException e) {
        log.log(Level.SEVERE, "error parsing operation from " + operationJson, e);
      }
    }
    return null;
  }

  private static ScanResult<Operation> parseScanResult(ScanResult<String> scanResult) {
    return new ScanResult<>(
        scanResult.getCursor(),
        newArrayList(
            transform(
                scanResult.getResult(), name -> Operation.newBuilder().setName(name).build())));
  }

  public ScanResult<Operation> scan(UnifiedJedis jedis, String cursor, int count) {
    return parseScanResult(operations.scan(jedis, cursor, count));
  }

  public ScanResult<Operation> findByInvocationId(
      UnifiedJedis jedis, String invocationId, String setCursor, int count) {
    return parseScanResult(toolInvocations.scan(jedis, invocationId, setCursor, count));
  }

  /**
   * @brief Insert an operation.
   * @details If the operation already exists, then it will be replaced.
   * @param jedis Jedis cluster client.
   * @param name name of operation.
   * @param operation Json of the operation.
   */
  public void insert(UnifiedJedis jedis, String name, String operation) {
    operations.insert(jedis, name, operation);
  }

  /**
   * @brief Remove an operation.
   * @details Deletes the operation.
   * @param jedis Jedis cluster client.
   * @param operationId The ID of the operation.
   */
  public void remove(UnifiedJedis jedis, String operationId) {
    operations.remove(jedis, operationId);
  }
}
