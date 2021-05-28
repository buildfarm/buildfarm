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

package build.buildfarm.common;

/**
 * @class ExecutionProperties
 * @brief Execution properties understood and used by buildfarm.
 * @details These are the execution property key names that have special meaning when applied to
 *     actions. Users can still configure their own unique execution properties along side these for
 *     workers and the operation queue.
 */
public class ExecutionProperties {
  /**
   * @field CORES
   * @brief The exec_property and platform property name for setting the core amount.
   * @details This is decided between client and server. The key value is expected to be an integer.
   */
  public static final String CORES = "cores";

  /**
   * @field MIN_CORES
   * @brief The exec_property and platform property name for setting min cores.
   * @details This is decided between client and server. The key value is expected to be an integer.
   */
  public static final String MIN_CORES = "min-cores";

  /**
   * @field MAX_CORES
   * @brief The exec_property and platform property name for setting max cores.
   * @details This is decided between client and server. The key value is expected to be an integer.
   */
  public static final String MAX_CORES = "max-cores";

  /**
   * @field MIN_MEM
   * @brief The exec_property and platform property name for setting mim memory usage.
   * @details This is decided between client and server. The key value is expected to be an integer.
   */
  public static final String MIN_MEM = "min-mem";

  /**
   * @field MAX_MEM
   * @brief The exec_property and platform property name for setting max memory usage.
   * @details This is decided between client and server. The key value is expected to be an integer.
   */
  public static final String MAX_MEM = "max-mem";

  /**
   * @field BLOCK_NETWORK
   * @brief The exec_property and platform property name for blocking network access.
   * @details This is decided between client and server. The key value is expected to be a boolean.
   */
  public static final String BLOCK_NETWORK = "block-network";

  /**
   * @field TMPFS
   * @brief The exec_property and platform property name for enabling tmpfs.
   * @details This is decided between client and server. The key value is expected to be a boolean.
   */
  public static final String TMPFS = "tmpfs";

  /**
   * @field ENV_VARS
   * @brief The exec_property and platform property name for providing additional environment
   *     variables.
   * @details This is decided between client and server. The key value should be a json dictionary,
   *     where each entry is a key/value representing env variable name and env variable value.
   */
  public static final String ENV_VARS = "env-vars";

  /**
   * @field ENV_VAR
   * @brief The exec_property and platform property prefix name for providing an additional
   *     environment variable.
   * @details This is decided between client and server. A colon is expected as a prefix key. The
   *     remaining part of the key is the env variable name, the value is the value of the env
   *     variable. example: exec_properties = {"env-var:FOO": "BAR"}
   */
  public static final String ENV_VAR = "env-var";

  /**
   * @field DEBUG_BEFORE_EXECUTION
   * @brief The exec_property and platform property name for indicating whether a user wants to
   *     debug the before action state of an execution.
   * @details This is intended to be used interactively to debug remote executions. The key value
   *     should be a boolean.
   */
  public static final String DEBUG_BEFORE_EXECUTION = "debug-before-execution";

  /**
   * @field DEBUG_AFTER_EXECUTION
   * @brief The exec_property and platform property name for indicating whether a user wants to get
   *     debug information from after the execution.
   * @details This is intended to be used interactively to debug remote executions. The key value
   *     should be a boolean.
   */
  public static final String DEBUG_AFTER_EXECUTION = "debug-after-execution";

  /**
   * @field DEBUG_TESTS_ONLY
   * @brief The exec_property and platform property name for indicating whether debug information
   *     should only be given for test actions.
   * @details This is intended to be used interactively to debug remote executions. The key value
   *     should be a boolean.
   */
  public static final String DEBUG_TESTS_ONLY = "debug-tests-only";

  /**
   * @field DEBUG_TARGET
   * @brief The exec_property and platform property name for indicating a specific target to debug.
   * @details This is intended to be used interactively to debug remote executions. The key value
   *     should be a string.
   */
  public static final String DEBUG_TARGET = "debug-target";

  /**
   * @field CHOOSE_QUEUE
   * @brief The exec_property to allow directly matching with a queue.
   * @details This is to support a paradigm where actions want to specifically request the queue to
   *     be placed in. Its less generic than having buildfarm choose the queue for you, and it leaks
   *     implementation details about how buildfarm is queuing your work. However, its desirable to
   *     match similar remote execution solutions that use exec_properties to choose which "pool"
   *     they want to run in.
   */
  public static final String CHOOSE_QUEUE = "choose-queue";

  /**
   * @field LINUX_SANDBOX
   * @brief The exec_property to inform the executor to use bazel's linux sandbox for actions.
   * @details In order to compare builds with and without the linux sandbox its helpful to have this
   *     property available. For example it could be set true as a global bazelrc option and this
   *     makes it easier to dynamically try different execution models without redeploying buildfarm
   *     with say different execution policies.
   */
  public static final String LINUX_SANDBOX = "linux-sandbox";

  /**
   * @field AS_NOBODY
   * @brief The exec_property to inform the executor to run the action as a 'nobody' user.
   * @details The "as nobody" functionality is supported by the bazel sandbox. This execution
   *     property may be fulfilled through the sandbox or a standalone program. This execution
   *     wrapper was previously used as a configured execution policy, but due to its involvement
   *     with the sandbox, we find it better to make its usage explicit in buildfarm and easier to
   *     test dynamically.
   */
  public static final String AS_NOBODY = "as-nobody";

  /**
   * @field PROCESS_WRAPPER
   * @brief The exec_property to inform the executor to run the action with the process-wrapper.
   * @details The "as nobody" functionality is supported by the bazel sandbox. This execution
   *     property may be fulfilled through the sandbox or a standalone program. This execution
   *     wrapper was previously used as a configured execution policy, but due to its involvement
   *     with the sandbox, we find it better to make its usage explicit in buildfarm and easier to
   *     test dynamically.
   */
  public static final String PROCESS_WRAPPER = "process-wrapper";
}
