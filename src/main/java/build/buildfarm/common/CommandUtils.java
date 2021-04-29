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

import build.bazel.remote.execution.v2.Command;
import com.google.common.collect.Iterables;

/**
 * @class Command
 * @brief Utilities for analyzing the Command proto message.
 * @details These utilities are used to derive information from REAPI's command type.
 */
public class CommandUtils {

  /**
   * @brief Derive if command is a test run.
   * @details Find a reliable way to identify whether a command is a test or not.
   * @param command The command to identify as a test command.
   * @return Whether the command is a test.
   * @note Suggested return identifier: exists.
   */
  public static boolean isTest(Command command) {
    // only tests are setting this currently - other mechanisms are unreliable
    return Iterables.any(
        command.getEnvironmentVariablesList(),
        (envVar) -> envVar.getName().equals("XML_OUTPUT_FILE"));
  }
}
