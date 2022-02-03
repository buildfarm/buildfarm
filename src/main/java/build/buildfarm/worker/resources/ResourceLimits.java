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

package build.buildfarm.worker.resources;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @class ResourceLimits
 * @brief Resource limitations imposed on specific actions.
 * @details These resource limitations are often specified by the client (via: exec_properties), but
 *     ultimately validated and decided by the server. Restricting the available resources for
 *     actions can have benefits on efficiency and stability as actions commonly share the same
 *     underlying resources. Keep in mind, that although workers will limit their resources for
 *     their actions, certain resource limitations may have already been taken into account in order
 *     to decide the eligibility of which workers can execute which actions.
 */
public class ResourceLimits {
  /**
   * @field workerName
   * @brief The name of the worker used for executing an action.
   * @details This can be helpful for correlating actions to the underlying worker agent.
   */
  public String workerName = "";

  /**
   * @field useLinuxSandbox
   * @brief Whether to use bazel's linux sandbox as an execution wrapper.
   * @details Other resource limits will be translated into the appropriate CLI arguments for the
   *     sandbox.
   */
  public boolean useLinuxSandbox = false;

  /**
   * @field useExecutionPolicies
   * @brief Whether to use the worker's configured execution policies.
   * @details Choosing a first-class execution wrapper, like the linux-sandbox, may decide to then
   *     ignore the existing execution policies.
   */
  public boolean useExecutionPolicies = true;

  /**
   * @field fakeUsername
   * @brief Whether the executor should fake the username of the action process.
   * @details This can be faked by using the "as-nobody" wrapper or fakeUsername in the
   *     linux-sandbox.
   */
  public boolean fakeUsername = false;

  /**
   * @field tmpFs
   * @brief Whether the action should use tmpfs for the tmp directory.
   * @details The linux-sandbox supports thie functionality through tmpfsDirs option.
   */
  public boolean tmpFs = false;

  /**
   * @field containerSettings
   * @brief Some actions may need to run in a container. These settings are used to determine which
   *     container to use and how to use it.
   * @details This also provides compatibility with https://github.com/bazelbuild/bazel-toolchains
   */
  public ContainerSettings containerSettings = new ContainerSettings();

  /**
   * @field cpu
   * @brief Resource limitations on CPUs.
   * @details Decides specific CPU limitations and whether to apply them for a given action.
   */
  public final CpuLimits cpu = new CpuLimits();

  /**
   * @field mem
   * @brief Resource limitations on memory usage.
   * @details Decides specific memory limitations and whether to apply them for a given action.
   */
  public final MemLimits mem = new MemLimits();

  /**
   * @field network
   * @brief Resource limitations on network usage.
   * @details Decides specific network limitations and whether to apply them for a given action.
   */
  public final NetworkLimits network = new NetworkLimits();

  /**
   * @field time
   * @brief Resource limitations on time usage.
   * @details Decides specific time limitations and whether to apply them for a given action.
   */
  public final TimeLimits time = new TimeLimits();

  /**
   * @field extraEnvironmentVariables
   * @brief Decides whether we should add extra environment variables when executing an operation.
   * @details These variables are added to the end of the existing environment variables in the
   *     Command.
   */
  public Map<String, String> extraEnvironmentVariables = new HashMap<>();

  /**
   * @field debugBeforeExecution
   * @brief If the user want to get debug information right before the actual execution.
   * @details This is a debugging flag and is not intended for normal execution.
   */
  public boolean debugBeforeExecution = false;

  /**
   * @field debugAfterExecution
   * @brief If the user want to get debug information right after the execution.
   * @details This is a debugging flag and is not intended for normal execution.
   */
  public boolean debugAfterExecution = false;

  /**
   * @field debugTestsOnly
   * @brief If the user only wants to get debug information for test actions.
   * @details When evaluating tests, regular actions are often needed to rebuild the test target
   *     first. This can cause the build to fail with debug information before evaluating the test
   *     action. To make it simpler, we can request debug information for tests only and not worry
   *     about getting debug information for regular build actions.
   */
  public boolean debugTestsOnly = true;

  /**
   * @field debugTarget
   * @brief A specific target to debug. Used for substring matching on actions.
   * @details When used, only matches will preserve debug flags.
   */
  public String debugTarget = "";

  /**
   * @field unusedProperties
   * @brief Exec_properties that were not used when deciding resource limits.
   * @details Foreign platform properties may be be added to the command that are ignored when
   *     parsing exec_properties. They are listed here for visibility.
   */
  public final Map<String, String> unusedProperties = new HashMap<>();

  /**
   * @field description
   * @brief Description explaining why settings were chosen.
   * @details This can be used to debug execution behavior.
   */
  public final ArrayList<String> description = new ArrayList<>();
}
