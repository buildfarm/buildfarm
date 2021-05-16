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

import static build.buildfarm.common.ExecutionProperties.*;

import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Platform.Property;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * @class ExecutionPropertiesParser
 * @brief Parse the user given exec_properties and construct an object for running commands.
 * @details The returned object reflects what properties were chosen / modified by the user.
 *     Buildfarm may further modify these choices based on its own configuration and constraints.
 */
public class ExecutionPropertiesParser {

  /**
   * @brief Decide resource limitations for the given command.
   * @details Platform properties from specified exec_properties are taken into account when
   *     constructing resource limits.
   * @param command The command to decide resource limitations for.
   * @return Suggested resource limits from user's exec_properties.
   * @note Suggested return identifier: resourceLimits.
   */
  public static ResourceLimits Parse(Command command) {

    // Build parser for all exec properties
    Map<String, BiConsumer<ResourceLimits, Property>> parser = new HashMap<>();
    parser.put(LINUX_SANDBOX, ExecutionPropertiesParser::storeLinuxSandbox);
    parser.put(MIN_CORES, ExecutionPropertiesParser::storeMinCores);
    parser.put(MAX_CORES, ExecutionPropertiesParser::storeMaxCores);
    parser.put(CORES, ExecutionPropertiesParser::storeCores);
    parser.put(MIN_MEM, ExecutionPropertiesParser::storeMinMem);
    parser.put(MAX_MEM, ExecutionPropertiesParser::storeMaxMem);
    parser.put(BLOCK_NETWORK, ExecutionPropertiesParser::storeBlockNetwork);
    parser.put(AS_NOBODY, ExecutionPropertiesParser::storeAsNobody);
    parser.put(ENV_VAR, ExecutionPropertiesParser::storeEnvVar);
    parser.put(ENV_VARS, ExecutionPropertiesParser::storeEnvVars);
    parser.put(DEBUG_BEFORE_EXECUTION, ExecutionPropertiesParser::storeBeforeExecutionDebug);
    parser.put(DEBUG_AFTER_EXECUTION, ExecutionPropertiesParser::storeAfterExecutionDebug);
    parser.put(DEBUG_TESTS_ONLY, ExecutionPropertiesParser::storeDebugTestsOnly);

    ResourceLimits limits = new ResourceLimits();
    command
        .getPlatform()
        .getPropertiesList()
        .forEach(
            (property) -> {
              evaluateProperty(parser, limits, property);
            });
    return limits;
  }

  /**
   * @brief Evaluate a given platform property of a command and use it to adjust execution settings.
   * @details Parses the property key/value and stores them appropriately.
   * @param parser The parser used to parse the given property and store it in the limits object.
   * @param limits Current limits to apply changes to.
   * @param property The property to store.
   */
  private static void evaluateProperty(
      Map<String, BiConsumer<ResourceLimits, Property>> parser,
      ResourceLimits limits,
      Property property) {

    // Most properties have the format "property_name => value".
    // But sometimes we need to pass both keys and values for a given property.
    // One might think to do the following:
    // "property_name => key:value"
    // However this is not sufficient as the client will overwrite each previous call to the
    // property name.
    // To avoid the client overwriting additional configurations, the property format becomes:
    // "property_name:key => value"
    // That's why we ignore delimiters when looking up the property name.
    String keyLookup = property.getName().split(":")[0];

    if (parser.containsKey(keyLookup)) {
      parser.get(keyLookup).accept(limits, property);
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
}
