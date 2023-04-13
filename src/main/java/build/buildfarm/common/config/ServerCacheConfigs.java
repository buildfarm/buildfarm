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

package build.buildfarm.common.config;

import lombok.Data;

/**
 * @class Server Cache Configs
 * @brief Configuration for the server's in memory caches.
 * @details The servers can cache different responses and REPAI data to provide better performance
 *     to the clients. It may come up at a cost of increased memory usage on the servers. Here we
 *     provide configuration on how large these caches can grow.
 */
@Data
public class ServerCacheConfigs {
  /**
   * @field directoryCacheMaxEntries
   * @brief The max number of entries that the directory cache will hold.
   * @details This will not dictate the max memory used.
   */
  public long directoryCacheMaxEntries = 64 * 1024;

  /**
   * @field commandCacheMaxEntries
   * @brief The max number of entries that the command cache will hold.
   * @details This will not dictate the max memory used.
   */
  public long commandCacheMaxEntries = 64 * 1024;

  /**
   * @field digestToActionCacheMaxEntries
   * @brief The max number of entries that the digest-to-action cache will hold.
   * @details This will not dictate the max memory used.
   */
  public long digestToActionCacheMaxEntries = 64 * 1024;

  /**
   * @field recentServedExecutionsCacheMaxEntries
   * @brief The max number of entries that the executions cache will hold.
   * @details This will not dictate the max memory used.
   */
  public long recentServedExecutionsCacheMaxEntries = 64 * 1024;
}
