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

import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Command.EnvironmentVariable;
import com.google.common.collect.Iterables;

/**
 * @class ResourceDecider
 * @brief Decide the resource limitations for a given command.
 * @details Platform properties from specified exec_properties are taken into account as well as
 *     global buildfarm configuration.
 */
public class ResourceDecider {
  /**
   * @brief Decide resource limitations for the given command.
   * @details Platform properties from specified exec_properties are taken into account as well as
   *     global buildfarm configuration.
   * @param command The command to decide resource limitations for.
   * @param onlyMulticoreTests Only allow tests to be multicore.
   * @param limitGlobalExecution Whether cpu limiting should be explicitly performed.
   * @param executeStageWidth The maximum amount of cores available for the operation.
   * @return Default resource limits.
   * @note Suggested return identifier: resourceLimits.
   */
  public static ResourceLimits decideResourceLimitations(
      Command command,
      boolean onlyMulticoreTests,
      boolean limitGlobalExecution,
      int executeStageWidth) {
    // Get all of the user suggested resource changes.
    ResourceLimits limits = ExecutionPropertiesParser.Parse(command);

    // Further modify the resource limits based on user selection and buildfarm constraints.
    adjustLimits(limits, command, onlyMulticoreTests, limitGlobalExecution, executeStageWidth);

    return limits;
  }

  /**
   * @brief Modify limits based on buildfarm constraints.
   * @details Existing limits configuration and buildfarm configuration are taken into account.
   * @param limits Existing limits chosen by the user's exec_properties.
   * @param command The command to decide resource limitations for.
   * @param onlyMulticoreTests Only allow tests to be multicore.
   * @param limitGlobalExecution Whether cpu limiting should be explicitly performed.
   * @param executeStageWidth The maximum amount of cores available for the operation.
   */
  public static void adjustLimits(
      ResourceLimits limits,
      Command command,
      boolean onlyMulticoreTests,
      boolean limitGlobalExecution,
      int executeStageWidth) {
    // force limits on non-test actions
    if (onlyMulticoreTests && !commandIsTest(command)) {
      limits.cpu.min = 1;
      limits.cpu.max = 1;
      limits.cpu.description.add(
          "cores restricted to 1 because this is enforced on non-test actions");
    }

    // avoid 0 cores when limiting
    if (limitGlobalExecution) {
      if (limits.cpu.min == 0) {
        limits.cpu.min = 1;
        limits.cpu.description.add(
            "min cores set to 1 as it cannot be 0 with limit global execution");
      }
      if (limits.cpu.max == 0) {
        limits.cpu.max = 1;
        limits.cpu.description.add(
            "max cores set to 1 as it cannot be 0 with limit global execution");
      }
    }

    // perform resource overrides based on test size
    TestSizeResourceOverrides overrides = new TestSizeResourceOverrides();
    if (overrides.enabled && commandIsTest(command)) {
      TestSizeResourceOverride override = deduceSizeOverride(command, overrides);
      limits.cpu.min = override.coreMin;
      limits.cpu.max = override.coreMax;
      limits.cpu.description.add(
          String.format(
              "cores are overridden due to test size (min=%s / max=%s",
              override.coreMin, override.coreMax));
    }

    adjustDebugFlags(command, limits);

    // Should we limit the cores of the action during execution? by default, no.
    // If the action has suggested core restrictions on itself, then yes.
    // Claim minimal core amount with regards to execute stage width.
    limits.cpu.limit = (limits.cpu.min > 0 || limits.cpu.max > 0);
    limits.cpu.claimed = Math.min(limits.cpu.min, executeStageWidth);

    // Should we limit the memory of the action during execution? by default, no.
    // If the action has suggested memory restrictions on itself, then yes.
    // Claim minimal memory amount based on action's suggestion.
    limits.mem.limit = (limits.mem.min > 0 || limits.mem.max > 0);
    limits.mem.claimed = limits.mem.min;

    // Avoid using the existing execution policies when using the linux sandbox.
    // Using these execution policies under the sandbox do not have the right permissions to work.
    // For the time being, we want to experiment with dynamically choosing the sandbox-
    // without affecting current configurations or relying on specific deployments.
    // This will dynamically skip using the worker configured execution policies.
    if (limits.useLinuxSandbox) {
      limits.useExecutionPolicies = false;
      limits.description.add("configured execution policies skipped because of choosing sandbox");
    }

    // we choose to resolve variables after the other variable values have been decided
    resolveEnvironmentVariables(limits);
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
    if (limits.debugTestsOnly && !commandIsTest(command)) {
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
   * @brief Derive if command is a test run.
   * @details Find a reliable way to identify whether a command is a test or not.
   * @param command The command to identify as a test command.
   * @return Whether the command is a test.
   * @note Suggested return identifier: exists.
   */
  private static boolean commandIsTest(Command command) {
    // only tests are setting this currently - other mechanisms are unreliable
    return Iterables.any(
        command.getEnvironmentVariablesList(),
        (envVar) -> envVar.getName().equals("XML_OUTPUT_FILE"));
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
