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
import build.bazel.remote.execution.v2.Platform.Property;
import build.buildfarm.common.ExecutionProperties;
import com.google.common.collect.Iterables;
import java.util.Map;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * @field operation
 * @brief The main operation object which contains digests to the remaining data members.
 * @details Its digests are used to resolve other data members.
 */

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
   * @param onlyMulticoreTests Only allow ttests to be multicore.
   * @param executeStageWidth The maximum amount of cores available for the operation.
   * @return Default resource limits.
   * @note Suggested return identifier: resourceLimits.
   */
  public static ResourceLimits decideResourceLimitations(
      Command command, boolean onlyMulticoreTests, int executeStageWidth) {
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

    // claim core amount according to execute stage width
    limits.cpu.claimed = Math.min(limits.cpu.min, executeStageWidth);

    // we choose to resolve variables after the other variable values have been decided
    resolveEnvironmentVariables(limits);

    return limits;
  }

  /**
   * @brief Evaluate a given platform property of a command and use it to adjust execution settings.
   * @details Parses the property key/value and stores them appropriately.
   * @param limits Current limits to apply changes to.
   * @param property The property to store.
   */
  private static void evaluateProperty(ResourceLimits limits, Property property) {
    // handle cpu properties
    if (property.getName().equals(ExecutionProperties.MIN_CORES)) {
      storeMinCores(limits, property);
    } else if (property.getName().equals(ExecutionProperties.MAX_CORES)) {
      storeMaxCores(limits, property);
    }

    // handle mem properties
    if (property.getName().equals(ExecutionProperties.MIN_MEM)) {
      storeMinMem(limits, property);
    } else if (property.getName().equals(ExecutionProperties.MAX_MEM)) {
      storeMaxMem(limits, property);
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
    }
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
    limits.mem.min = Integer.parseInt(property.getValue());
  }

  /**
   * @brief Store the property for max mem.
   * @details Parses and stores the property.
   * @param limits Current limits to apply changes to.
   * @param property The property to store.
   */
  private static void storeMaxMem(ResourceLimits limits, Property property) {
    limits.mem.max = Integer.parseInt(property.getValue());
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
}
