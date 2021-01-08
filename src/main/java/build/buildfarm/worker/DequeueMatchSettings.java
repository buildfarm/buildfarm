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

package build.buildfarm.worker;

/**
 * @class DequeueMatchSettings
 * @brief Settings used by a worker to determine whether they should keep dequeued operations.
 * @details This determines whether workers keep operations or decide to requeue them for a
 *     different worker.
 */
public class DequeueMatchSettings {

  /**
   * @field acceptEverything
   * @brief Whether or not the worker should accept everything it gets off the queue.
   * @details This will assume the worker can always execute operations from the queue it matches
   *     with.
   */
  public boolean acceptEverything = false;

  /**
   * @field allowUnmatched
   * @brief Whether or not the worker should accept platform properties that it does not match with.
   * @details This is often necessary if the queue is also configured to allow unmatched properties.
   */
  public boolean allowUnmatched = false;
}
