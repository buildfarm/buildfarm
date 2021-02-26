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

import build.bazel.remote.execution.v2.ActionResult;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import com.google.rpc.Code;

///
/// @class   ExecutionDebugger
/// @brief   The execution debugger allows users to print relevant debug
///          information related to their action's execution.
/// @details From the client's perspective, its not always clear what
///          buildfarm is doing with an action. Users need a way to understand
///          what their actions look like before buildfarm executes them. As
///          well as specific debug information from after the execution. This
///          can also be helpful in debugging buildfarm itself.
///
public class ExecutionDebugger {

  ///
  /// @brief   Fail the operation before executing it but provide relevant
  ///          debug information to the user via a failed result.
  /// @details This allows users to see relevant debug information related to
  ///          the executor.
  /// @param   processBuilder Information about the constructed process.
  /// @param   limits         The resource limitations of an execution.
  /// @param   resultBuilder  Used to report back debug information.
  /// @return  Return code for the debugged execution.
  /// @note    Suggested return identifier: code.
  ///
  public static Code performBeforeExecutionDebug(
      ProcessBuilder processBuilder, ResourceLimits limits, ActionResult.Builder resultBuilder) {
    String message = getBeforeExecutionDebugInfo(processBuilder, limits, resultBuilder);
    resultBuilder.setStderrRaw(ByteString.copyFromUtf8(message));
    resultBuilder.setExitCode(-1);
    return Code.OK;
  }
  ///
  /// @brief   Fail the operation after executing it but provide relevant debug
  ///          information to the user via a failed result.
  /// @details This allows users to see relevant debug information related to
  ///          the executor.
  /// @param   processBuilder Information about the constructed process.
  /// @param   limits         The resource limitations of an execution.
  /// @param   resultBuilder  Used to report back debug information.
  /// @return  Return code for the debugged execution.
  /// @note    Suggested return identifier: code.
  ///
  public static Code performAfterExecutionDebug(
      ProcessBuilder processBuilder, ResourceLimits limits, ActionResult.Builder resultBuilder) {
    String message = getAfterExecutionDebugInfo(processBuilder, limits, resultBuilder);
    resultBuilder.setStderrRaw(ByteString.copyFromUtf8(message));
    resultBuilder.setExitCode(-1);
    return Code.OK;
  }
  ///
  /// @brief   Build the debug log message that we want users to see.
  /// @details This be sent back to the user via the stderr of their execution.
  /// @param   processBuilder Information about the constructed process.
  /// @param   limits         The resource limitations of an execution.
  /// @param   resultBuilder  Used to report back debug information.
  /// @return  The debug information to show the user.
  /// @note    Suggested return identifier: debugMessage.
  ///
  private static String getBeforeExecutionDebugInfo(
      ProcessBuilder processBuilder, ResourceLimits limits, ActionResult.Builder resultBuilder) {
    String message = "Buildfarm debug information before execution:\n";
    Gson gson = new Gson();
    message += String.join(" ", processBuilder.command()) + "\n";
    message += gson.toJson(limits);
    return message;
  }
  ///
  /// @brief   Build the debug log message that we want users to see.
  /// @details This be sent back to the user via the stderr of their execution.
  /// @param   processBuilder Information about the constructed process.
  /// @param   limits         The resource limitations of an execution.
  /// @param   resultBuilder  Used to report back debug information.
  /// @return  The debug information to show the user.
  /// @note    Suggested return identifier: debugMessage.
  ///
  private static String getAfterExecutionDebugInfo(
      ProcessBuilder processBuilder, ResourceLimits limits, ActionResult.Builder resultBuilder) {
    String message = "Buildfarm debug information after execution:\n";
    Gson gson = new Gson();
    message += String.join(" ", processBuilder.command()) + "\n";
    message += gson.toJson(limits);
    return message;
  }
}
