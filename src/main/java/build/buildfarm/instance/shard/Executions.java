// Copyright 2023 The Buildfarm Authors. All rights reserved.
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
import javax.annotation.Nullable;
import lombok.extern.java.Log;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.resps.ScanResult;

/**
 * @class Executions
 * @brief Stores all executions that have taken place on the system.
 * @details We keep track of them in the distributed state to avoid them getting lost if a
 *     particular machine goes down. They should also exist for some period of time after a build
 *     invocation has finished so that developers can lookup the status of their build and
 *     information about the executions that ran.
 */
@Log
public class Executions {
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
   * @field executions
   * @brief A mapping from executionName -> operation
   * @details Operation names are unique.
   */
  public RedisMap executions;

  /**
   * @field actions
   * @brief A mapping from actionKey -> operationName
   * @details ActionKeys which may produce execution merges.
   */
  public RedisMap actions;

  /**
   * @brief Constructor.
   * @details Construct container for executions.
   * @param name The global name of the execution map.
   * @param actionsName The global name of the actions map.
   * @param timeout_s When to expire executions.
   */
  public Executions(RedisSetMap toolInvocations, String name, String actionsName, int timeout_s) {
    this.toolInvocations = toolInvocations;
    executions = new RedisMap(name, timeout_s);
    actions = new RedisMap(actionsName, timeout_s);
  }

  /**
   * @brief Get the execution operation by name.
   * @details If the execution does not exist, null is returned.
   * @param jedis Jedis cluster client.
   * @param executionName The name of the execution.
   * @return The json of the operation. null if key does not exist.
   * @note Overloaded.
   * @note Suggested return identifier: operation.
   */
  public Operation get(UnifiedJedis jedis, String name) {
    return parse(executions.get(jedis, name));
  }

  /**
   * @brief Get the executions by executionNames.
   * @details If the execution does not exist, null is returned.
   * @param jedis Jedis cluster client.
   * @param names The names of the executions.
   * @return The json of the executions. null if the execution does not exist.
   * @note Overloaded.
   * @note Suggested return identifier: operations.
   */
  public Iterable<Operation> get(UnifiedJedis jedis, Iterable<String> names) {
    return transform(executions.get(jedis, names), entry -> Executions.parse(entry.getValue()));
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

  private ScanResult<Operation> parseScanResult(UnifiedJedis jedis, ScanResult<String> scanResult) {
    return new ScanResult<>(
        scanResult.getCursor(),
        newArrayList(
            transform(
                executions.get(jedis, scanResult.getResult()), entry -> parse(entry.getValue()))));
  }

  public ScanResult<Operation> scan(UnifiedJedis jedis, String cursor, int count) {
    return parseScanResult(jedis, executions.scan(jedis, cursor, count));
  }

  public ScanResult<Operation> findByToolInvocationId(
      UnifiedJedis jedis, String toolInvocationId, String setCursor, int count) {
    return parseScanResult(jedis, toolInvocations.scan(jedis, toolInvocationId, setCursor, count));
  }

  public void insert(UnifiedJedis jedis, String name, String operationJson) {
    executions.insert(jedis, name, operationJson);
  }

  /**
   * @brief Insert an execution.
   * @details If the execution already exists, then it will be replaced.
   * @param jedis Jedis cluster client.
   * @param name name of operation.
   * @param operationJson Json of the operation.
   */
  public boolean create(UnifiedJedis jedis, String actionKey, String name, String operationJson) {
    executions.insert(jedis, name, operationJson);
    if (!actions.putIfAbsent(jedis, actionKey, name)) {
      return false;
    }
    return true;
  }

  /**
   * @brief Remove an execution.
   * @details Deletes the execution.
   * @param jedis Jedis cluster client.
   * @param name The name of the execution.
   */
  public void remove(UnifiedJedis jedis, String name) {
    executions.remove(jedis, name);
  }

  public @Nullable Operation merge(UnifiedJedis jedis, String actionKey) {
    String name = actions.get(jedis, actionKey);
    if (name == null) {
      return null;
    }
    return get(jedis, name);
  }

  public void unmerge(UnifiedJedis jedis, String actionKey) {
    actions.remove(jedis, actionKey);
  }
}
