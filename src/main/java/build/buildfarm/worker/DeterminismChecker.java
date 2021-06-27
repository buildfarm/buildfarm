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

import build.bazel.remote.execution.v2.ActionResult;
import build.buildfarm.worker.resources.ResourceLimits;
import com.google.devtools.build.lib.shell.Protos.ExecutionStatistics;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.protobuf.ByteString;
import com.google.rpc.Code;

/**
 * @class DeterminismChecker
 * @brief Run an action multiple times and fail it if its outputs are not deterministic in content
 * @details The determinism checker can be used by the client to find nondeterministic actions and sources of cache positioning.
 */
public class DeterminismChecker {
  /**
   * @brief Check the determinism of an action by running it multiple times and comparing the outputs of each execution.
   * @details This allows clients to find nondeterministic actions and sources of cache positioning.
   * @param processBuilder The process to run.
   * @param limits The resource limitations of an execution (contains determinism check settings).
   * @param resultBuilder Used to report back debug information.
   * @return Return code for the debugged execution.
   * @note Suggested return identifier: code.
   */
  public static Code checkDeterminism(
      ProcessBuilder processBuilder, ResourceLimits limits, ActionResult.Builder resultBuilder) {
    String message = getDeterminismResults(processBuilder, limits, resultBuilder);
    resultBuilder.setStderrRaw(ByteString.copyFromUtf8(message));
    resultBuilder.setExitCode(-1);
    return Code.OK;
  }
  
  private static String getDeterminismResults(ProcessBuilder processBuilder, ResourceLimits limits, ActionResult.Builder resultBuilder){
    return "";
  }
}