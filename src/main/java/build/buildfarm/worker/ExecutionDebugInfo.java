// Copyright 2021 The Buildfarm Authors. All rights reserved.
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

import build.buildfarm.worker.resources.ResourceLimits;
import com.google.devtools.build.lib.shell.Protos.ResourceUsage;
import java.util.HashMap;
import java.util.Map;

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
   * @field environment
   * @brief The environment variables for the command.
   * @details The environment variables are decided by both users and buildfarm.
   */
  public Map<String, String> environment = new HashMap<>();

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

  /**
   * @field executionStatistics
   * @brief The resource usage statistics from running the action.
   * @details These statistics are calculated through POSIX getrusage()- a feature available to
   *     bazel's linux sandbox.
   */
  public ResourceUsage executionStatistics = ResourceUsage.newBuilder().build();

  /**
   * @field stdout
   * @brief The action result's stdout
   * @details Converted from proto bytes.
   */
  public String stdout = "";

  /**
   * @field stdout
   * @brief The action result's stdout
   * @details Converted from proto bytes.
   */
  public String stderr = "";

  /**
   * @field exitCode
   * @brief The exit code of the execution.
   * @details The debugger will fail the execution but this is to indicate what the actual exit code
   *     of the action was.
   */
  public int exitCode = 0;
}
