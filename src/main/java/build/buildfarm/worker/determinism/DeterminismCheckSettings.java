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

package build.buildfarm.worker.determinism;

import build.buildfarm.worker.OperationContext;
import build.buildfarm.worker.WorkerContext;
import build.buildfarm.worker.resources.ResourceLimits;

/**
 * @class DeterminismCheckSettings
 * @brief Information used for testing whether an action is deterministic.
 * @details The executor can use this context information to execute an action multiple times and
 *     discover if it is deterministic.
 */
public class DeterminismCheckSettings {
  /**
   * @field workerContext
   * @brief Contains information about an action and where its results exist on a worker.
   * @details We need to be able to get queued the operation and control of the exec filesystem.
   */
  public WorkerContext workerContext;

  /**
   * @field operationContext
   * @brief Contains information about an action and where its results exist on a worker.
   * @details We need the queue entry, command, and execution directory.
   */
  public OperationContext operationContext;

  /**
   * @field processBuilder
   * @brief This represents the process that we are going to test the determinism of.
   * @details We expect the process itself to have already been built up by the caller.
   */
  public ProcessBuilder processBuilder;

  /**
   * @field limits
   * @brief Resource limit information about the process to run.
   * @details This has determinism running information as one of the resource properties.
   */
  public ResourceLimits limits;
}
