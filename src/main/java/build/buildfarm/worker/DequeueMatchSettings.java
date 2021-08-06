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
   * @field refuseWhenExpiringCas
   * @brief Whether or not the worker should refuse operation when it is expiring the CAS.
   * @details This can be enabled for performance reasons when the disk is being heavily utilized
   *     from CAS expiration.
   */
  public boolean refuseWhenExpiringCas = false;

  /**
   * @field refuseOnMismatchedProperties
   * @brief Whether or not the worker should refuse operation when properties mismatch.
   * @details This is generally not needed as the workers already match with the correct queue.
   */
  public boolean refuseOnMismatchedProperties = false;

  /**
   * @field allowUnmatched
   * @brief Whether or not the worker should accept platform properties that it does not match with.
   * @details This is often necessary if the queue is also configured to allow unmatched properties.
   */
  public boolean allowUnmatched = false;
}
