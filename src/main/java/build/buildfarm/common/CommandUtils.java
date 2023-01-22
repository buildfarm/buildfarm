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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

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
    return command.getEnvironmentVariablesList().stream()
        .anyMatch((envVar) -> envVar.getName().equals("XML_OUTPUT_FILE"));
  }

  /**
   * @brief Get the command's output paths.
   * @details This abstracts over REAPI changes around how this information is encoded in a Command.
   * @param command The command to get output paths from.
   * @param actionRoot The action root used to resolve the Command's relative paths.
   * @return The list of output paths.
   * @note Suggested return identifier: output_paths.
   */
  public static List<Path> getResolvedOutputPaths(Command command, Path actionRoot) {
    // REAPI clients previously needed to specify whether the output path was a directory or file.
    // This turned out to be too restrictive-- some build tools don't know what an action produces
    // until it is done.
    // https://github.com/bazelbuild/remote-apis/issues/75
    // That's unfortunate for server implementations, because now buildfarm has to distinguish
    // between files and paths itself.

    // To quote REAPI:
    // "New in v2.1: this field supersedes the DEPRECATED `output_files` and
    // `output_directories` fields. If `output_paths` is used, `output_files` and
    // `output_directories` will be ignored!"
    // https://github.com/bazelbuild/remote-apis/blob/3a21deee813d0b98aaeef9737c720e509e10dc8b/build/bazel/remote/execution/v2/remote_execution.proto#L651-L653
    List<Path> resolvedPaths = new ArrayList<>();

    // If `output_paths` is used, `output_files` and
    // `output_directories` will be ignored!"
    if (command.getOutputPathsCount() != 0) {
      for (String outputPath : command.getOutputPathsList()) {
        resolvedPaths.add(actionRoot.resolve(outputPath));
      }
      return resolvedPaths;
    }

    // Assuming `output_paths` was not used,
    // fetch deprecated `output_files` and `output_directories` for backwards compatibility.
    for (String outputPath : command.getOutputFilesList()) {
      resolvedPaths.add(actionRoot.resolve(outputPath));
    }
    for (String outputPath : command.getOutputDirectoriesList()) {
      resolvedPaths.add(actionRoot.resolve(outputPath));
    }

    return resolvedPaths;
  }
}
