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

import build.buildfarm.common.redis.RedisMap;
import java.util.Map;
import java.util.Set;
import redis.clients.jedis.JedisCluster;

/**
 * @class Operations
 * @brief Stores all operations that have taken place on the system.
 * @details We keep track of them in the distributed state to avoid them getting lost if a
 *     particular machine goes down. They should also exist for some period of time after a build
 *     invocation has finished so that developers can lookup the status of their build and
 *     information about the operations that ran.
 */
public class Operations {
  /**
   * @field operationIds
   * @brief A mapping from operationID -> operation
   * @details OperationIDs are unique.
   */
  public RedisMap operationIds;

  /**
   * @brief Constructor.
   * @details Construct container for operations.
   * @param name The global name of the operation map.
   * @param timeout_s When to expire operations.
   */
  public Operations(String name, int timeout_s) {
    operationIds = new RedisMap(name, timeout_s);
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
  public String get(JedisCluster jedis, String operationId) {
    return operationIds.get(jedis, operationId);
  }

  /**
   * @brief Get the operations by operationIDs.
   * @details If the operation does not exist, null is returned.
   * @param jedis Jedis cluster client.
   * @param searchIds The IDs of the operations.
   * @return The json of the operations. null if the operation does not exist.
   * @note Overloaded.
   * @note Suggested return identifier: operations.
   */
  public Iterable<Map.Entry<String, String>> get(JedisCluster jedis, Iterable<String> searchIds) {
    return operationIds.get(jedis, searchIds);
  }

  /**
   * @brief Get the operation by invocationId.
   * @details If the invocation does not exist an empty set is returned.
   * @param jedis Jedis cluster client.
   * @param operationId The ID of the invocation.
   * @return A set of operation IDs.
   * @note Overloaded.
   * @note Suggested return identifier: operationIds.
   */
  public Set<String> getByInvocationId(JedisCluster jedis, String invocationId) {
    return jedis.smembers(invocationId);
  }

  /**
   * @brief Insert an operation.
   * @details If the operation already exists, then it will be replaced.
   * @param jedis Jedis cluster client.
   * @param invocationId ID of the invocation that the operation is a part of.
   * @param operationId ID of operation.
   * @param operation Json of the operation.
   */
  public void insert(
      JedisCluster jedis, String invocationId, String operationId, String operation) {
    operationIds.insert(jedis, operationId, operation);

    // We also store a mapping from invocationID -> operationIDs
    // This is a common lookup that needs to be performant.
    jedis.sadd(invocationId, operationId);
  }

  /**
   * @brief Remove an operation.
   * @details Deletes the operation.
   * @param jedis Jedis cluster client.
   * @param operationId The ID of the operation.
   */
  public void remove(JedisCluster jedis, String operationId) {
    operationIds.remove(jedis, operationId);
  }
}
