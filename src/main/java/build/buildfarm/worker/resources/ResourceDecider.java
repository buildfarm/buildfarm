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

package build.buildfarm.worker.resources;

import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Command.EnvironmentVariable;
import build.buildfarm.common.CommandUtils;
import build.buildfarm.common.config.SandboxSettings;
import org.apache.commons.lang3.StringUtils;

/**
 * @class ResourceDecider
 * @brief Decide the resource limitations for a given command.
 * @details Platform properties from specified exec_properties are taken into account as well as
 *     global buildfarm configuration.
 */
public final class ResourceDecider {
  /**
   * @brief Decide resource limitations for the given command.
   * @details Platform properties from specified exec_properties are taken into account as well as
   *     global buildfarm configuration.
   * @param command The command to decide resource limitations for.
   * @param workerName The name of the worker taking on the action.
   * @param onlyMulticoreTests Only allow tests to be multicore.
   * @param defaultMaxCores The unspecified maximum constraint for cores.
   * @param limitGlobalExecution Whether cpu limiting should be explicitly performed.
   * @param executeStageWidth The maximum amount of cores available for the operation.
   * @param allowBringYourOwnContainer Whether or not the feature of "bringing your own containers"
   *     is allowed.
   * @param sandbox Settings on whether to use a sandbox."
   * @return Default resource limits.
   * @note Suggested return identifier: resourceLimits.
   */
  public static ResourceLimits decideResourceLimitations(
      Command command,
      String workerName,
      int defaultMaxCores,
      boolean onlyMulticoreTests,
      boolean limitGlobalExecution,
      int executeStageWidth,
      boolean allowBringYourOwnContainer,
      SandboxSettings sandbox) {
    // Get all of the user suggested resource changes.
    ResourceLimits limits = ExecutionPropertiesParser.Parse(command);

    // Further modify the resource limits based on user selection and buildfarm constraints.
    adjustLimits(
        limits,
        command,
        workerName,
        defaultMaxCores,
        onlyMulticoreTests,
        limitGlobalExecution,
        executeStageWidth,
        allowBringYourOwnContainer,
        sandbox);

    return limits;
  }

  /**
   * @brief Modify limits based on buildfarm constraints.
   * @details Existing limits configuration and buildfarm configuration are taken into account.
   * @param limits Existing limits chosen by the user's exec_properties.
   * @param command The command to decide resource limitations for.
   * @param workerName The name of the worker taking on the action.
   * @param defaultMaxCores The unspecified maximum constraint for cores.
   * @param onlyMulticoreTests Only allow tests to be multicore.
   * @param limitGlobalExecution Whether cpu limiting should be explicitly performed.
   * @param executeStageWidth The maximum amount of cores available for the operation.
   * @param allowBringYourOwnContainer Whether or not the feature of "bringing your own containers"
   *     is allowed.
   * @param sandbox Settings on whether to use a sandbox."
   */
  private static void adjustLimits(
      ResourceLimits limits,
      Command command,
      String workerName,
      int defaultMaxCores,
      boolean onlyMulticoreTests,
      boolean limitGlobalExecution,
      int executeStageWidth,
      boolean allowBringYourOwnContainer,
      SandboxSettings sandbox) {
    // store worker name
    limits.workerName = workerName;

    // force limits on non-test actions
    if (onlyMulticoreTests && !CommandUtils.isTest(command)) {
      if (limits.cpu.min > 1 || limits.cpu.max > defaultMaxCores) {
        limits.cpu.description.add(
            String.format(
                "cores restricted to %d because this is enforced on non-test actions",
                defaultMaxCores));
      }
      limits.cpu.min = 1;
      limits.cpu.max = defaultMaxCores;
    } else if (limits.cpu.max <= 0) {
      if (defaultMaxCores > 0) {
        limits.cpu.description.add(
            String.format("cores restricted to %d by default", defaultMaxCores));
      }
      limits.cpu.max = defaultMaxCores;
    }

    // avoid 0 cores, just in general, since it informs our claim
    if (limits.cpu.min <= 0) {
      limits.cpu.min = 1;
      limits.cpu.description.add(
          "min cores set to 1 as it cannot be 0 with limit global execution");
    }

    // compel a specified max to be <= executeStageWidth
    if (limits.cpu.max > 0) {
      if (limits.cpu.max > executeStageWidth) {
        limits.cpu.description.add(String.format("max cores limited to %d", executeStageWidth));
      }
      limits.cpu.max = Math.min(limits.cpu.max, executeStageWidth);
    }

    // compel the range to be min <= max
    if (limits.cpu.max > 0) {
      if (limits.cpu.min > limits.cpu.max) {
        limits.cpu.description.add(
            String.format(
                "min cores %d limited to specified max %d", limits.cpu.min, limits.cpu.max));
      }
      limits.cpu.min = Math.min(limits.cpu.max, limits.cpu.min);
    }

    // perform resource overrides based on test size
    TestSizeResourceOverrides overrides = new TestSizeResourceOverrides();
    if (overrides.enabled && CommandUtils.isTest(command)) {
      TestSizeResourceOverride override = deduceSizeOverride(command, overrides);
      limits.cpu.min = override.coreMin;
      limits.cpu.max = override.coreMax;
      limits.cpu.description.add(
          String.format(
              "cores are overridden due to test size (min=%d / max=%d",
              override.coreMin, override.coreMax));
    }

    adjustDebugFlags(command, limits);

    // Should we limit the cores of the action during execution? by default, per
    // limitGlobalExecution.
    // Otherwise, if the action has suggested core restrictions on itself, then yes.
    // Claim minimal core amount with regards to execute stage width.
    limits.cpu.limit =
        limitGlobalExecution || (limits.cpu.max > 0 && limits.cpu.max < executeStageWidth);
    limits.cpu.claimed = Math.min(limits.cpu.min, executeStageWidth);

    // Should we limit the memory of the action during execution? by default, no.
    // If the action has suggested memory restrictions on itself, then yes.
    // Claim minimal memory amount based on action's suggestion.
    limits.mem.limit = limits.mem.max > 0;
    limits.mem.claimed = limits.mem.min;

    // There's certain functionality that is NOT possible without using the linux sandbox.
    // To avoid confusing users, we will enable the sandbox for them automatically if they request
    // certain execution constraints. If these constraints can be enforced through other means in
    // the future, than picking the sandbox automatically can be removed. For now however, its
    // better to choose the sandbox in order to honor the user's request instead of silently
    // ignoring the request due to a disabled sandbox.
    decideSandboxUsage(limits, sandbox);

    // Avoid using the existing execution policies when using the linux sandbox.
    // Using these execution policies under the sandbox do not have the right permissions to work.
    // For the time being, we want to experiment with dynamically choosing the sandbox-
    // without affecting current configurations or relying on specific deployments.
    // This will dynamically skip using the worker configured execution policies.
    if (limits.useLinuxSandbox) {
      limits.useExecutionPolicies = false;
      limits.description.add("configured execution policies skipped because of choosing sandbox");
    }

    // Decide whether the action will run in a container
    if (allowBringYourOwnContainer && !limits.containerSettings.containerImage.isEmpty()) {
      // enable container execution
      limits.containerSettings.enabled = true;

      // Adjust additional flags for when a container is being used.
      adjustContainerFlags(limits);
    }

    // we choose to resolve variables after the other variable values have been decided
    resolveEnvironmentVariables(limits);
  }

  private static void decideSandboxUsage(ResourceLimits limits, SandboxSettings sandbox) {
    // configured on
    if (sandbox.isAlwaysUse()) {
      limits.useLinuxSandbox = true;
      limits.description.add("enabled");
      return;
    }

    // selected based on other features
    if (limits.network.blockNetwork && sandbox.isSelectForBlockNetwork()) {
      limits.useLinuxSandbox = true;
      limits.description.add("sandbox is chosen because of block network usage");
    }
    if (limits.tmpFs && sandbox.isSelectForTmpFs()) {
      limits.useLinuxSandbox = true;
      limits.description.add("sandbox is chosen because of tmpfs usage");
    }
  }

  private static void adjustContainerFlags(ResourceLimits limits) {
    // bazelbuild/bazel-toolchains provides container images that start with "docker://".
    // However, docker is unable to fetch images that have this as a prefix in the URI.
    // Our solution is to remove the prefix when we see it.
    // https://github.com/bazelbuild/bazel-buildfarm/issues/1060
    limits.containerSettings.containerImage =
        StringUtils.removeStart(limits.containerSettings.containerImage, "docker://");

    // Avoid using the existing execution policies when running actions under docker.
    // The programs used in the execution policies likely won't exist in the container images.
    limits.useExecutionPolicies = false;
    limits.description.add("configured execution policies skipped because of choosing docker");

    // avoid limiting resources as cgroups may not be available in the container.
    // in fact, we will use docker's cgroup settings explicitly.
    // TODO(thickey): use docker's cgroup settings given existing resource limitations.
    limits.cpu.limit = false;
    limits.mem.limit = false;
    limits.description.add("resource limiting disabled because of choosing docker");
  }

  private static void adjustDebugFlags(Command command, ResourceLimits limits) {
    if (!limits.debugTarget.isEmpty()) {
      handleTargetDebug(command, limits);
    } else {
      handleTestDebug(command, limits);
    }
  }

  private static void handleTargetDebug(Command command, ResourceLimits limits) {
    // When debugging particular targets, disable debugging on non-matches.
    if (!commandMatchesDebugTarget(command, limits)) {
      limits.debugBeforeExecution = false;
      limits.debugAfterExecution = false;
      limits.description.add("debugging is disabled because target is not matched");
    }
  }

  private static void handleTestDebug(Command command, ResourceLimits limits) {
    // When debugging tests, disable debugging on non-tests.
    if (limits.debugTestsOnly && !CommandUtils.isTest(command)) {
      limits.debugBeforeExecution = false;
      limits.debugAfterExecution = false;
      limits.description.add("debugging is disabled because only tests are enabled for debugging");
    }
  }

  private static boolean commandMatchesDebugTarget(Command command, ResourceLimits limits) {
    for (String argument : command.getArgumentsList()) {
      if (argument.contains(limits.debugTarget)) {
        return true;
      }
    }
    return false;
  }

  /**
   * @brief Resolve any templates found in the env variables.
   * @details This assumes the other values that will be resolving the templates have already been
   *     decided.
   * @param limits Current limits to have resolved.
   */
  private static void resolveEnvironmentVariables(ResourceLimits limits) {
    // resolve any template values
    limits.extraEnvironmentVariables.replaceAll(
        (key, val) -> {
          val = val.replace("{{limits.cpu.min}}", String.valueOf(limits.cpu.min));
          val = val.replace("{{limits.cpu.max}}", String.valueOf(limits.cpu.max));
          val = val.replace("{{limits.cpu.claimed}}", String.valueOf(limits.cpu.claimed));
          return val;
        });
  }

  /**
   * @brief Get resource overrides by analyzing the test command for it's "test size".
   * @details test size is defined as an environment variable.
   * @param command The test command to derive the size of.
   * @return The resource overrides corresponding to the command's test size.
   * @note Suggested return identifier: overrides.
   */
  private static TestSizeResourceOverride deduceSizeOverride(
      Command command, TestSizeResourceOverrides overrides) {
    for (EnvironmentVariable envVar : command.getEnvironmentVariablesList()) {
      if (envVar.getName().equals("TEST_SIZE")) {
        if (envVar.getValue().equals("small")) {
          return overrides.small;
        }
        if (envVar.getValue().equals("medium")) {
          return overrides.medium;
        }
        if (envVar.getValue().equals("large")) {
          return overrides.large;
        }
        if (envVar.getValue().equals("enormous")) {
          return overrides.enormous;
        }
      }
    }

    return overrides.unknown;
  }
}
