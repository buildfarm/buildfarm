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
 * @class ExecutionDebugInfo
 * @brief All debug information provided by buildfarm when debugging the execution of an action.
 * @details This information can be returned to the client as the stderr of the failed action.
 */
public class ExecutionDebugInfo {

  /**
   * @field description
   * @brief A description of the data for how it was populated.
   * @details This is useful for knowing if it was populated before/after the execution.
   */
  public String description = "";

  /**
   * @field command
   * @brief The command that buildfarm is running for the action.
   * @details The original command of the action is often modified with execution wrappers.
   */
  public String command = "";

  /**
   * @field workingDirectory
   * @brief The working directory when running the action.
   * @details This correlates the command field.
   */
  public String workingDirectory = "";

  /**
   * @field limits
   * @brief The resource limits imposed on the action.
   * @details These limitations are decided by exec_properties and buildfarm configurations.
   */
  public ResourceLimits limits = new ResourceLimits();
}
