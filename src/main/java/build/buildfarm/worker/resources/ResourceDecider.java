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
import build.bazel.remote.execution.v2.Platform.Property;
import build.buildfarm.common.ExecutionProperties;
import com.google.common.collect.Iterables;
import java.util.Map;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

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
    ResourceLimits limits = new ResourceLimits();

    command
        .getPlatform()
        .getPropertiesList()
        .forEach(
            (property) -> {
              evaluateProperty(limits, property);
            });

    // force limits on non-test actions
    if (onlyMulticoreTests && !commandIsTest(command)) {
      limits.cpu.min = 1;
      limits.cpu.max = 1;
    }

    if (limitGlobalExecution) {
      limits.cpu.min = Math.max(limits.cpu.min, 1);
      limits.cpu.max = Math.max(limits.cpu.max, 1);
    }

    // perform resource overrides based on test size
    TestSizeResourceOverrides overrides = new TestSizeResourceOverrides();
    if (overrides.enabled && commandIsTest(command)) {
      TestSizeResourceOverride override = deduceSizeOverride(command, overrides);
      limits.cpu.min = override.coreMin;
      limits.cpu.max = override.coreMax;
    }

    adjustDebugFlags(command, limits);

    // Should we limit the cores of the action during execution? by default, no.
    // If the action has suggested core restrictions on itself, then yes.
    // Claim minimal core amount with regards to execute stage width.
    limits.cpu.limit = (limits.cpu.min > 0 || limits.cpu.max > 0) || limitGlobalExecution;
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
    }

    // we choose to resolve variables after the other variable values have been decided
    resolveEnvironmentVariables(limits);

    return limits;
  }

  private static void adjustDebugFlags(Command command, ResourceLimits limits) {

    if (!limits.debugTarget.isEmpty()) {
      if (!commandMatchesDebugTarget(command, limits)) {
        limits.debugBeforeExecution = false;
        limits.debugAfterExecution = false;
      }

    } else {

      // adjust debugging based on whether its a test
      if (limits.debugTestsOnly && !commandIsTest(command)) {
        limits.debugBeforeExecution = false;
        limits.debugAfterExecution = false;
      }
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
   * @brief Evaluate a given platform property of a command and use it to adjust execution settings.
   * @details Parses the property key/value and stores them appropriately.
   * @param limits Current limits to apply changes to.
   * @param property The property to store.
   */
  private static void evaluateProperty(ResourceLimits limits, Property property) {

    // handle execution wrapper properties
    if (property.getName().equals(ExecutionProperties.LINUX_SANDBOX)) {
      storeLinuxSandbox(limits, property);
    }

    // handle cpu properties
    if (property.getName().equals(ExecutionProperties.MIN_CORES)) {
      storeMinCores(limits, property);
    } else if (property.getName().equals(ExecutionProperties.MAX_CORES)) {
      storeMaxCores(limits, property);
    } else if (property.getName().equals(ExecutionProperties.CORES)) {
      storeCores(limits, property);
    }

    // handle mem properties
    if (property.getName().equals(ExecutionProperties.MIN_MEM)) {
      storeMinMem(limits, property);
    } else if (property.getName().equals(ExecutionProperties.MAX_MEM)) {
      storeMaxMem(limits, property);
    }

    // handle network properties
    if (property.getName().equals(ExecutionProperties.BLOCK_NETWORK)) {
      storeBlockNetwork(limits, property);
    }

    // handle user properties
    if (property.getName().equals(ExecutionProperties.AS_NOBODY)) {
      storeAsNobody(limits, property);
    }

    // handle env properties
    else if (property.getName().equals(ExecutionProperties.ENV_VARS)) {
      storeEnvVars(limits, property);
    } else if (property.getName().startsWith(ExecutionProperties.ENV_VAR)) {
      storeEnvVar(limits, property);
    }

    // handle debug properties
    else if (property.getName().equals(ExecutionProperties.DEBUG_BEFORE_EXECUTION)) {
      storeBeforeExecutionDebug(limits, property);
    } else if (property.getName().equals(ExecutionProperties.DEBUG_AFTER_EXECUTION)) {
      storeAfterExecutionDebug(limits, property);
    } else if (property.getName().equals(ExecutionProperties.DEBUG_TESTS_ONLY)) {
      storeDebugTestsOnly(limits, property);
    } else if (property.getName().equals(ExecutionProperties.DEBUG_TARGET)) {
      storeDebugTarget(limits, property);
    }
  }

  /**
   * @brief Store the property for both min/max cores.
   * @details Parses and stores the property.
   * @param limits Current limits to apply changes to.
   * @param property The property to store.
   */
  private static void storeCores(ResourceLimits limits, Property property) {
    int amount = Integer.parseInt(property.getValue());
    limits.cpu.min = amount;
    limits.cpu.max = amount;
  }

  /**
   * @brief Store the property for using bazel's linux sandbox.
   * @details Parses and stores a boolean.
   * @param limits Current limits to apply changes to.
   * @param property The property to store.
   */
  private static void storeLinuxSandbox(ResourceLimits limits, Property property) {
    limits.useLinuxSandbox = Boolean.parseBoolean(property.getValue());
  }

  /**
   * @brief Store the property for min cores.
   * @details Parses and stores the property.
   * @param limits Current limits to apply changes to.
   * @param property The property to store.
   */
  private static void storeMinCores(ResourceLimits limits, Property property) {
    limits.cpu.min = Integer.parseInt(property.getValue());
  }

  /**
   * @brief Store the property for max cores.
   * @details Parses and stores the property.
   * @param limits Current limits to apply changes to.
   * @param property The property to store.
   */
  private static void storeMaxCores(ResourceLimits limits, Property property) {
    limits.cpu.max = Integer.parseInt(property.getValue());
  }

  /**
   * @brief Store the property for min mem.
   * @details Parses and stores the property.
   * @param limits Current limits to apply changes to.
   * @param property The property to store.
   */
  private static void storeMinMem(ResourceLimits limits, Property property) {
    limits.mem.min = Long.parseLong(property.getValue());
  }

  /**
   * @brief Store the property for max mem.
   * @details Parses and stores the property.
   * @param limits Current limits to apply changes to.
   * @param property The property to store.
   */
  private static void storeMaxMem(ResourceLimits limits, Property property) {
    limits.mem.max = Long.parseLong(property.getValue());
  }

  /**
   * @brief Store the property for blocking network.
   * @details Parses and stores a boolean.
   * @param limits Current limits to apply changes to.
   * @param property The property to store.
   */
  private static void storeBlockNetwork(ResourceLimits limits, Property property) {
    limits.network.blockNetwork = Boolean.parseBoolean(property.getValue());
  }

  /**
   * @brief Store the property for faking username.
   * @details Parses and stores a boolean.
   * @param limits Current limits to apply changes to.
   * @param property The property to store.
   */
  private static void storeAsNobody(ResourceLimits limits, Property property) {
    limits.fakeUsername = Boolean.parseBoolean(property.getValue());
  }

  /**
   * @brief Store the property for env vars.
   * @details Parses the property as json.
   * @param limits Current limits to apply changes to.
   * @param property The property to store.
   */
  private static void storeEnvVars(ResourceLimits limits, Property property) {
    try {
      JSONParser parser = new JSONParser();
      limits.extraEnvironmentVariables = (Map<String, String>) parser.parse(property.getValue());
    } catch (ParseException pe) {
    }
  }

  /**
   * @brief Store the property for an env var.
   * @details Parses the property key name for the env var name.
   * @param limits Current limits to apply changes to.
   * @param property The property to store.
   */
  private static void storeEnvVar(ResourceLimits limits, Property property) {
    String keyValue[] = property.getName().split(":", 2);
    String key = keyValue[1];
    String value = property.getValue();
    limits.extraEnvironmentVariables.put(key, value);
  }

  /**
   * @brief Store the property for debugging before an execution.
   * @details Parses and stores a boolean.
   * @param limits Current limits to apply changes to.
   * @param property The property to store.
   */
  private static void storeBeforeExecutionDebug(ResourceLimits limits, Property property) {
    limits.debugBeforeExecution = Boolean.parseBoolean(property.getValue());
  }

  /**
   * @brief Store the property for debugging after an execution.
   * @details Parses and stores a boolean.
   * @param limits Current limits to apply changes to.
   * @param property The property to store.
   */
  private static void storeAfterExecutionDebug(ResourceLimits limits, Property property) {
    limits.debugAfterExecution = Boolean.parseBoolean(property.getValue());
  }

  /**
   * @brief Store the property for debugging tests only.
   * @details Parses and stores a boolean.
   * @param limits Current limits to apply changes to.
   * @param property The property to store.
   */
  private static void storeDebugTestsOnly(ResourceLimits limits, Property property) {
    limits.debugTestsOnly = Boolean.parseBoolean(property.getValue());
  }

  /**
   * @brief Store the property for debugging a target.
   * @details Parses and stores a String.
   * @param limits Current limits to apply changes to.
   * @param property The property to store.
   */
  private static void storeDebugTarget(ResourceLimits limits, Property property) {
    limits.debugTarget = property.getValue();
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
