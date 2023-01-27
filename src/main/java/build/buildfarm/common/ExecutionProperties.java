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
 *     workers and the operation queue. Some of these properties originated specifically for
 *     buildfarm usage. Other properties exist for compadibility with bazelbuild/bazel-toolchains.
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
   * @field FAKE_HOSTNAME
   * @brief The exec_property and platform property name for fake hostname.
   * @details This is decided between client and server. The key value is expected to be a boolean.
   */
  public static final String FAKE_HOSTNAME = "fake-hostname";

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
   *     variable. example: exec_properties = {"env-var:FOO": "BAR"} We do not need to include the
   *     colon in the property name. The parser handles this detection.
   */
  public static final String ENV_VAR = "env-var";

  /**
   * @field SKIP_SLEEP
   * @brief The exec_property and platform property prefix name for skipping sleep calls in the
   *     action.
   * @details This affects the following syscalls: nanosleep, clock_nanosleep, select, poll,
   *     gettimeofday, clock_gettime, time. This useful for finding artificially slow actions.
   */
  public static final String SKIP_SLEEP = "skip-sleep";

  /**
   * @field TIME_SHIFT
   * @brief The exec_property and platform property prefix name for shifting time before running an
   *     action.
   * @details Currently only supports shifting time into the future.
   */
  public static final String TIME_SHIFT = "time-shift";

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

  /**
   * @field CONTAINER_IMAGE
   * @brief The exec_property to inform the executor to run the action under the specified
   *     container.
   * @details Originated from bazelbuild/bazel-toolchains.
   */
  public static final String CONTAINER_IMAGE = "container-image";

  /**
   * @field DOCKER_ADD_CAPABILITIES
   * @brief The exec_property to add docker capabilities.
   * @details Originated from bazelbuild/bazel-toolchains.
   */
  public static final String DOCKER_ADD_CAPABILITIES = "dockerAddCapabilities";

  /**
   * @field DOCKER_DROP_CAPABILITIES
   * @brief The exec_property to remove docker capabilities.
   * @details Originated from bazelbuild/bazel-toolchains.
   */
  public static final String DOCKER_DROP_CAPABILITIES = "dockerDropCapabilities";

  /**
   * @field DOCKER_NETWORK
   * @brief The exec_property to configure docker's network.
   * @details Originated from bazelbuild/bazel-toolchains.
   */
  public static final String DOCKER_NETWORK = "dockerNetwork";

  /**
   * @field DOCKER_PRIVILEGED
   * @brief The exec_property to run docker as privileged.
   * @details Originated from bazelbuild/bazel-toolchains.
   */
  public static final String DOCKER_PRIVILEGED = "dockerPrivileged";

  /**
   * @field DOCKER_RUN_AS_ROOT
   * @brief The exec_property to run docker as root.
   * @details Originated from bazelbuild/bazel-toolchains.
   */
  public static final String DOCKER_RUN_AS_ROOT = "dockerRunAsRoot";

  /**
   * @field DOCKER_RUNTIME
   * @brief The exec_property to set docker's runtime.
   * @details Originated from bazelbuild/bazel-toolchains.
   */
  public static final String DOCKER_RUNTIME = "dockerRuntime";

  /**
   * @field DOCKER_SHM_SIZE
   * @brief The exec_property to set docker's shm size.
   * @details Originated from bazelbuild/bazel-toolchains.
   */
  public static final String DOCKER_SHM_SIZE = "dockerShmSize";

  /**
   * @field DOCKER_SIBLING_CONTAINERS
   * @brief The exec_property to enable docker sibling containers.
   * @details Originated from bazelbuild/bazel-toolchains.
   */
  public static final String DOCKER_SIBLING_CONTAINERS = "dockerSiblingContainers";

  /**
   * @field DOCKER_ULIMITS
   * @brief The exec_property to set docker ulimits.
   * @details Originated from bazelbuild/bazel-toolchains.
   */
  public static final String DOCKER_ULIMITS = "dockerUlimits";

  /**
   * @field DOCKER_USE_URANDOM
   * @brief The exec_property to enable docker's use of urandom.
   * @details Originated from bazelbuild/bazel-toolchains.
   */
  public static final String DOCKER_USE_URANDOM = "dockerUseURandom";

  /**
   * @field GCE_MACHINE_TYPE
   * @brief The exec_property to choose a particular GCE machine type.
   * @details Originated from bazelbuild/bazel-toolchains.
   */
  public static final String GCE_MACHINE_TYPE = "gceMachineType";

  /**
   * @field OS_FAMILY
   * @brief The exec_property to choose which OS family the action should run under.
   * @details Originated from bazelbuild/bazel-toolchains.
   */
  public static final String OS_FAMILY = "OSFamily";

  /**
   * @field POOL
   * @brief The exec_property to choose which pool of workers should take the action.
   * @details Originated from bazelbuild/bazel-toolchains (similar to choose-queue for buildfarm's
   *     operation queue).
   */
  public static final String POOL = "Pool";
}
