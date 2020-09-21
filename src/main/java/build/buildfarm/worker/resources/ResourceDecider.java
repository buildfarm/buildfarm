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

///
/// @class   ResourceDecider
/// @brief   Decide the resource limitations for a given command.
/// @details Platform properties from specified exec_properties are taken
///          into account as well as global buildfarm configuration.
///
public class ResourceDecider {

  ///
  /// @field   EXEC_PROPERTY_MIN_CORES
  /// @brief   The exec_property and platform property name for setting min
  ///          cores.
  /// @details This is decided between client and server.
  ///
  private static final String EXEC_PROPERTY_MIN_CORES = "min-cores";

  ///
  /// @field   EXEC_PROPERTY_MAX_CORES
  /// @brief   The exec_property and platform property name for setting max
  ///          cores.
  /// @details This is decided between client and server.
  ///
  private static final String EXEC_PROPERTY_MAX_CORES = "max-cores";

  ///
  /// @brief   Decide resource limitations for the given command.
  /// @details Platform properties from specified exec_properties are taken
  ///          into account as well as global buildfarm configuration.
  /// @param   command            The command to decide resource limitations for.
  /// @param   onlyMulticoreTests Only allow ttests to be multicore.
  /// @return  Default resource limits.
  /// @note    Suggested return identifier: resourceLimits.
  ///
  public static ResourceLimits decideResourceLimitations(
      Command command, boolean onlyMulticoreTests) {
    ResourceLimits limits = getDefaultLimitations();

    setCpuLimits(limits, command, onlyMulticoreTests);

    return limits;
  }
  ///
  /// @brief   Decide CPU limitations.
  /// @details Given a default set of limitations, use the command and global
  ///          configuration to adjust CPU limitations.
  /// @param   limits             Current limits to apply changes to.
  /// @param   command            The command to decide resource limitations.
  /// @param   onlyMulticoreTests Only allow ttests to be multicore.
  ///
  private static void setCpuLimits(
      ResourceLimits limits, Command command, boolean onlyMulticoreTests) {
    // apply cpu limits specified on command
    limits.cpu.min = getIntegerPlatformValue(command, EXEC_PROPERTY_MIN_CORES, limits.cpu.min);
    limits.cpu.max = getIntegerPlatformValue(command, EXEC_PROPERTY_MAX_CORES, limits.cpu.max);

    // force limits on non-test actions
    if (onlyMulticoreTests && !commandIsTest(command)) {
      limits.cpu.min = 1;
      limits.cpu.max = 1;
    }
  }
  ///
  /// @brief   Get default resource limits.
  /// @details Get the default resource limits before adjusting based on
  ///          action's exec_properties global configuration.
  /// @return  Default resource limits.
  /// @note    Suggested return identifier: resourceLimits.
  ///
  private static ResourceLimits getDefaultLimitations() {
    // These can be moved to configuration in the future
    ResourceLimits limits = new ResourceLimits();

    // supported
    limits.cpu.limit = true;
    limits.cpu.min = 1;
    limits.cpu.max = 1;

    // not supported
    limits.gpu.limit = false;
    limits.gpu.min = 0;
    limits.gpu.max = 0;

    // not supported
    limits.mem.limit = false;
    limits.mem.min_gb = 4;
    limits.mem.max_gb = 32;

    // not supported
    limits.disk.limit = false;
    limits.disk.min_mb = 2 * 1024;
    limits.disk.max_mb = 2 * 1024;

    // not supported
    limits.network.restrict = false;

    // supported elsewhere
    limits.time.timeout_s = 10 * 60;

    return limits;
  }
  ///
  /// @brief   Get an integer value from a platform property.
  /// @details Get the first value of the property name given. If the property
  ///          name does not exist the default provided is returned.
  /// @param   command    The command to extract the platform value from.
  /// @param   name       The platform property name.
  /// @param   defaultVal The default value if the property name does not exist.
  /// @return  The decided platform value.
  /// @note    Suggested return identifier: platformValue.
  ///
  private static int getIntegerPlatformValue(Command command, String name, int defaultVal) {
    for (Property property : command.getPlatform().getPropertiesList()) {
      if (property.getName().equals(name)) {
        return Integer.parseInt(property.getValue());
      }
    }
    return defaultVal;
  }
  ///
  /// @brief   Whether the command has the platform property.
  /// @details Checks whether the command has the given platform property name.
  /// @param   command The command to extract the platform value from.
  /// @param   name    The platform property name.
  /// @return  Whether the platform name exists.
  /// @note    Suggested return identifier: exists.
  ///
  private static boolean hasPlatformProperty(Command command, String name) {
    for (Property property : command.getPlatform().getPropertiesList()) {
      if (property.getName().equals(name)) {
        return true;
      }
    }
    return false;
  }
  ///
  /// @brief   Derive if command is a test run.
  /// @details Find a reliable way to identify whether a command is a test or
  ///          not.
  /// @param   command The command to identify as a test command.
  /// @return  Whether the command is a test.
  /// @note    Suggested return identifier: exists.
  ///
  private static boolean commandIsTest(Command command) {
    // only tests are setting this currently - other mechanisms are unreliable
    return Iterables.any(
        command.getEnvironmentVariablesList(),
        (envVar) -> envVar.getName().equals("XML_OUTPUT_FILE"));
  }
}
