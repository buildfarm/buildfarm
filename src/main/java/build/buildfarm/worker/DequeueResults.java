// Copyright 2021 The Bazel Authors. All rights reserved.
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

package build.buildfarm.worker;

/**
 * @class DequeueResults
 * @brief The results of evaluating a dequeued operation.
 * @details Contains information about whether the operation should be kept and if resources were
 *     claimed.
 */
public class DequeueResults {
  /**
   * @field keep
   * @brief Whether the operation should be kept.
   * @details Operations not kept should be put back on the queue.
   */
  public boolean keep = false;

  /**
   * @field resourcesClaimed
   * @brief Whether resources have been claimed while deciding to keep operation.
   * @details If resources are claimed but the operation is not kept, the resources should be
   *     released.
   */
  public boolean resourcesClaimed = false;
}
