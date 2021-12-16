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

import java.util.List;

/**
 * @class CasIndexSettings
 * @brief Settings used to determine how to index CAS entries and remove worker entries.
 * @details These are used for reindexing when a worker is leaving the cluster.
 */
public class CasIndexSettings {
  /**
   * @field hostName
   * @brief The name of the worker.
   * @details This correlates the the worker that needs removed from CAS entries.
   */
  public List<String> hostNames;

  /**
   * @field scanAmount
   * @brief The number of redis entries to scan at a time.
   * @details Larger amounts will be faster but require more memory.
   */
  public int scanAmount;

  /**
   * @field casQuery
   * @brief How to query all of the CAS entries in redis.
   * @details The cas key is a global buildfarm config.
   */
  public String casQuery;
}
