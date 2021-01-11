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
import com.google.common.collect.Iterables;
import java.util.Collections;
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
   * @field EXEC_PROPERTY_MIN_CORES
   * @brief The exec_property and platform property name for setting min cores.
   * @details This is decided between client and server.
   */
  private static final String EXEC_PROPERTY_MIN_CORES = "min-cores";

  /**
   * @field EXEC_PROPERTY_MAX_CORES
   * @brief The exec_property and platform property name for setting max cores.
   * @details This is decided between client and server.
   */
  private static final String EXEC_PROPERTY_MAX_CORES = "max-cores";

  /**
   * @field EXEC_PROPERTY_ENV_VARS
   * @brief The exec_property and platform property name for providing additional environment
   *     variables.
   * @details This is decided between client and server.
   */
  private static final String EXEC_PROPERTY_ENV_VARS = "env-vars";

  /**
   * @field EXEC_PROPERTY_ENV_VAR
   * @brief The exec_property and platform property prefix name for providing an additional
   *     environment variable.
   * @details This is decided between client and server.
   */
  private static final String EXEC_PROPERTY_ENV_VAR = "env-var:";

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
    ResourceLimits limits = getDefaultLimitations();

    setCpuLimits(limits, command, onlyMulticoreTests, executeStageWidth);
    setEnvironmentVariables(limits, command, onlyMulticoreTests);

    return limits;
  }

  /**
   * @brief Decide CPU limitations.
   * @details Given a default set of limitations, use the command and global configuration to adjust
   *     CPU limitations.
   * @param limits Current limits to apply changes to.
   * @param command The command to decide resource limitations.
   * @param onlyMulticoreTests Only allow ttests to be multicore.
   * @param executeStageWidth The maximum amount of cores available for the operation.
   */
  private static void setCpuLimits(
      ResourceLimits limits, Command command, boolean onlyMulticoreTests, int executeStageWidth) {
    // apply cpu limits specified on command
    limits.cpu.min = getIntegerPlatformValue(command, EXEC_PROPERTY_MIN_CORES, limits.cpu.min);
    limits.cpu.max = getIntegerPlatformValue(command, EXEC_PROPERTY_MAX_CORES, limits.cpu.max);

    // force limits on non-test actions
    if (onlyMulticoreTests && !commandIsTest(command)) {
      limits.cpu.min = 1;
      limits.cpu.max = 1;
    }

    // claim core amount according to execute stage width
    limits.cpu.claimed = Math.min(limits.cpu.min, executeStageWidth);
  }

  /**
   * @brief Decide extra environment variables.
   * @details Given a default set of limitations, use the command and global configuration to adjust
   *     the extra environment variables.
   * @param limits Current limits to apply changes to.
   * @param command The command to decide resource limitations.
   * @param onlyMulticoreTests Only allow ttests to be multicore.
   */
  private static void setEnvironmentVariables(
      ResourceLimits limits, Command command, boolean onlyMulticoreTests) {
    // parse any user given environment variables into a map
    // if the json is malformed assume no environment variables were given
    try {
      JSONParser parser = new JSONParser();
      limits.extraEnvironmentVariables =
          (Map<String, String>)
              parser.parse(getStringPlatformValue(command, EXEC_PROPERTY_ENV_VARS, "{}"));
    } catch (ParseException pe) {
    }

    addIndividualEnvVars(limits, command);

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
   * @brief Extend env variables that were individual passed.
   * @details These are discovered by identifying execution property key names.
   * @param limits Current limits to apply changes to.
   * @param command The command to decide resource limitations.
   */
  private static void addIndividualEnvVars(ResourceLimits limits, Command command) {
    command
        .getPlatform()
        .getPropertiesList()
        .forEach(
            (property) -> {
              if (property.getName().startsWith(EXEC_PROPERTY_ENV_VAR)) {
                String keyValue[] = property.getName().split(":", 2);
                String key = keyValue[1];
                String value = property.getValue();
                limits.extraEnvironmentVariables.put(key, value);
              }
            });
  }

  /**
   * @brief Get default resource limits.
   * @details Get the default resource limits before adjusting based on action's exec_properties
   *     global configuration.
   * @return Default resource limits.
   * @note Suggested return identifier: resourceLimits.
   */
  private static ResourceLimits getDefaultLimitations() {
    // These can be moved to configuration in the future
    ResourceLimits limits = new ResourceLimits();

    // we usually prefer to isolate operations through some kind of visualization (cgroups)
    // and then limit their execution to a single core.  When necessary, user's request more cores.
    limits.cpu = new CpuLimits();
    limits.cpu.limit = true;
    limits.cpu.min = 1;
    limits.cpu.max = 1;
    limits.cpu.claimed = 1;

    // Sometimes a client needs to add extra environment variables to their execution.
    // If they are unable to set these in their code, and --action_env is not sufficient,
    // they may choose to annotate extra environment variables this way.
    // these environment variables can be templated, which allows them to reference
    // other values related to their execution.
    // for example, a client may want certain rules to set environment variables
    // based on what buildfarm decides to limit the core count to.
    // that could look like this:
    // "OMP_NUM_THREADS": "{{limits.cpu.claimed}}"
    // "MKL_NUM_THREADS": "{{limits.cpu.claimed}}"
    limits.extraEnvironmentVariables = Collections.emptyMap();

    return limits;
  }

  /**
   * @brief Get an integer value from a platform property.
   * @details Get the first value of the property name given. If the property name does not exist
   *     the default provided is returned.
   * @param command The command to extract the platform value from.
   * @param name The platform property name.
   * @param defaultVal The default value if the property name does not exist.
   * @return The decided platform value.
   * @note Suggested return identifier: platformValue.
   */
  private static int getIntegerPlatformValue(Command command, String name, int defaultVal) {
    for (Property property : command.getPlatform().getPropertiesList()) {
      if (property.getName().equals(name)) {
        return Integer.parseInt(property.getValue());
      }
    }
    return defaultVal;
  }

  /**
   * @brief Get a string value from a platform property.
   * @details Get the first value of the property name given. If the property name does not exist
   *     the default provided is returned.
   * @param command The command to extract the platform value from.
   * @param name The platform property name.
   * @param defaultVal The default value if the property name does not exist.
   * @return The decided platform value.
   * @note Suggested return identifier: platformValue.
   */
  private static String getStringPlatformValue(Command command, String name, String defaultVal) {
    for (Property property : command.getPlatform().getPropertiesList()) {
      if (property.getName().equals(name)) {
        return property.getValue();
      }
    }
    return defaultVal;
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
