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

package build.buildfarm.instance.shard;

import com.google.protobuf.Duration;

/**
 * @class OperationQueuerSettings
 * @brief These settings are for determining how to transfer elements from the prequeue to the
 *     operation queue.
 * @details The server is responsible for transferring prequeue elements to the operation queue. The
 *     details of this transformation can be configured here.
 */
public class OperationQueuerSettings {
  /**
   * @field run
   * @brief Whether we should run the operation queuer.
   * @details If true, we will run the operation queuer on a separate thread. If false, we will not
   *     run the operation queuer and none of these other settings will be relevant.
   */
  public boolean run = true;

  /**
   * @field operationWriteTimeout
   * @brief How long should we wait to write a QueuedOperation to the CAS?
   * @details This timeout determines when we give up trying to write a particular QueuedOperation
   *     to the CAS. Failure to write operations may result in prequeue congestion.
   */
  public Duration operationWriteTimeout;

  /**
   * @field pollFrequency
   * @brief The polling frequency for updating the expiration status of a an operation we are
   *     currently trying to queue.
   * @details This could be half the watch expiry.
   */
  public Duration pollFrequency;

  /**
   * @field pollTimeout
   * @brief How long to wait before giving up on polling.
   * @details This duration represents a deadline from when the poller is created.
   */
  public Duration pollTimeout;

  /**
   * @field queueConcurrency
   * @brief How many operations can the operation queuer work on concurrently?
   * @details Keep in mind, this does not guarantee any amount of parallelism and will depend on the
   *     executor service.
   */
  public int queueConcurrency = 0;
}
