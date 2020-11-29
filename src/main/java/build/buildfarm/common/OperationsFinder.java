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

import redis.clients.jedis.JedisCluster;

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

    return results;
  }
}
