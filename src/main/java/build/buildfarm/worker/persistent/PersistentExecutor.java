// Copyright 2023 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.worker.persistent;

import static java.lang.String.join;

import build.bazel.remote.execution.v2.ActionResult;
import build.buildfarm.worker.resources.ResourceLimits;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.devtools.build.lib.worker.WorkerProtocol.Input;
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkRequest;
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkResponse;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.rpc.Code;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import lombok.extern.java.Log;
import persistent.bazel.client.WorkerKey;

/**
 * Executes an Action like Executor/DockerExecutor, writing to ActionResult.
 *
 * <p>Currently has special code for discriminating between Javac/Scalac, and other persistent
 * workers, likely for debugging purposes, but need to revisit. (Can't remember fully since it was
 * so long ago!)
 */
@Log
public class PersistentExecutor {
  private static final ProtoCoordinator coordinator =
      ProtoCoordinator.ofCommonsPool(getMaxWorkersPerKey());

  // TODO load from config (i.e. {worker_root}/persistent)
  public static final Path defaultWorkRootsDir = Path.of("/tmp/worker/persistent/");

  public static final String PERSISTENT_WORKER_FLAG = "--persistent_worker";

  // TODO Revisit hardcoded actions
  static final String JAVABUILDER_JAR =
      "external/remote_java_tools/java_tools/JavaBuilder_deploy.jar";

  private static final String SCALAC_EXEC_NAME = "Scalac";
  private static final String JAVAC_EXEC_NAME = "JavaBuilder";

  // How many workers can exist at once for a given WorkerKey
  // There may be multiple WorkerKeys per mnemonic,
  //  e.g. if builds are run with different tool fingerprints
  private static final int defaultMaxWorkersPerKey = 6;

  private static int getMaxWorkersPerKey() {
    try {
      return Integer.parseInt(System.getenv("BUILDFARM_MAX_WORKERS_PER_KEY"));
    } catch (Exception ignored) {
      log.info(
          "Could not get env var BUILDFARM_MAX_WORKERS_PER_KEY; defaulting to "
              + defaultMaxWorkersPerKey);
    }
    return defaultMaxWorkersPerKey;
  }

  /**
   * Run some Action on a Persistent Worker.
   *
   * <ol>
   *   <li>Parses action inputs into tool inputs and request inputs
   *   <li>Makes the WorkerKey
   *   <li>Loads the tool inputs, if needed, into the WorkerKey tool inputs dir
   *   <li>Runs the work request on its Coordinator, passing it the required context
   *   <li>Passes output to the resultBuilder
   * </ol>
   *
   * @param context
   * @param operationName
   * @param argsList
   * @param envVars
   * @param limits
   * @param timeout
   * @param workRootsDir
   * @param resultBuilder
   * @return
   */
  public static Code runOnPersistentWorker(
      WorkFilesContext context,
      String operationName,
      ImmutableList<String> argsList,
      ImmutableMap<String, String> envVars,
      ResourceLimits limits,
      Duration timeout,
      Path workRootsDir,
      ActionResult.Builder resultBuilder)
      throws IOException {
    // Pull out persistent worker start command from the overall action request

    log.log(Level.FINE, "executeCommandOnPersistentWorker[" + operationName + "]");
    String persistentWorkerInitCmd = generatePersistentWorkerCommand(argsList);
    ImmutableList<String> initCmd = parseInitCmd(persistentWorkerInitCmd, argsList);

    String executionName = getExecutionName(argsList);
    if (executionName.isEmpty()) {
      log.log(Level.SEVERE, "Invalid Argument: " + argsList);
      return Code.INVALID_ARGUMENT;
    }

    // TODO revisit why this was necessary in the first place
    // (@wiwa) I believe the reason has to do with JavaBuilder workers not relying on env vars,
    // as compared to rules_scala, only reading info from the argslist of each command.
    // That would mean the Java worker keys should be invariant to the env vars we see.
    ImmutableMap<String, String> env;
    if (executionName.equals(JAVAC_EXEC_NAME)) {
      env = ImmutableMap.of();
    } else {
      env = envVars;
    }

    int requestArgsIdx = initCmd.size();
    ImmutableList<String> workerExecCmd = initCmd;
    ImmutableList<String> workerInitArgs =
        ImmutableList.<String>builder().add(PERSISTENT_WORKER_FLAG).build();
    ImmutableList<String> requestArgs = argsList.subList(requestArgsIdx, argsList.size());

    // Make Key

    WorkerInputs workerFiles = WorkerInputs.from(context, requestArgs);

    Path binary = Path.of(workerExecCmd.getFirst());
    if (!workerFiles.containsTool(binary) && !binary.isAbsolute()) {
      throw new IllegalArgumentException(
          "Binary wasn't a tool input nor an absolute path: " + binary);
    }

    WorkerKey key =
        Keymaker.make(
            context.opRoot,
            workRootsDir,
            workerExecCmd,
            workerInitArgs,
            env,
            executionName,
            workerFiles);

    coordinator.copyToolInputsIntoWorkerToolRoot(key, workerFiles);

    // Make request

    // Inputs should be relative paths (if they are from operation root)
    ImmutableList.Builder<Input> reqInputsBuilder = ImmutableList.builder();

    for (Map.Entry<Path, Input> opInput : workerFiles.allInputs.entrySet()) {
      Input relInput = opInput.getValue();
      Path opPath = opInput.getKey();
      if (opPath.startsWith(workerFiles.opRoot)) {
        relInput =
            relInput.toBuilder().setPath(workerFiles.opRoot.relativize(opPath).toString()).build();
      }
      reqInputsBuilder.add(relInput);
    }
    ImmutableList<Input> reqInputs = reqInputsBuilder.build();

    WorkRequest request =
        WorkRequest.newBuilder()
            .addAllArguments(requestArgs)
            .addAllInputs(reqInputs)
            .setRequestId(0)
            .build();

    RequestCtx requestCtx = new RequestCtx(request, context, workerFiles, timeout);

    // Run request
    // Required file operations (in/out) are the responsibility of the coordinator

    log.log(Level.FINE, "Request with key: " + key);
    WorkResponse response;
    String stdErr = "";
    try {
      ResponseCtx fullResponse = coordinator.runRequest(key, requestCtx);

      response = fullResponse.response;
      stdErr = fullResponse.errorString;
    } catch (Exception e) {
      String debug =
          "\n\tRequest.initCmd: "
              + workerExecCmd
              + "\n\tRequest.initArgs: "
              + workerInitArgs
              + "\n\tRequest.requestArgs: "
              + request.getArgumentsList();
      String msg = "Exception while running request: " + e + debug + "\n\n";

      log.log(Level.SEVERE, msg, e);

      response =
          WorkResponse.newBuilder()
              .setOutput(msg)
              .setExitCode(-1) // incomplete
              .build();
    }

    // Set results

    String responseOut = response.getOutput();
    log.log(Level.FINE, "WorkResponse.output: " + responseOut);

    int exitCode = response.getExitCode();
    resultBuilder
        .setExitCode(exitCode)
        .setStdoutRaw(response.getOutputBytes())
        .setStderrRaw(ByteString.copyFrom(stdErr, StandardCharsets.UTF_8));
    return Code.OK;
  }

  private static ImmutableList<String> parseInitCmd(String cmdStr, ImmutableList<String> argsList) {
    if (!cmdStr.endsWith(PERSISTENT_WORKER_FLAG)) {
      throw new IllegalArgumentException(
          "Persistent Worker request must contain "
              + PERSISTENT_WORKER_FLAG
              + "\nGot: parseInitCmd["
              + cmdStr
              + "]"
              + "\n"
              + argsList);
    }

    String cmd =
        cmdStr.trim().substring(0, (cmdStr.length() - PERSISTENT_WORKER_FLAG.length()) - 1);

    // Parse init command into list of space-separated words, without the persistent worker flag
    ImmutableList.Builder<String> initCmdBuilder = ImmutableList.builder();
    for (String s : argsList) {
      if (cmd.isEmpty()) {
        break;
      }
      cmd = cmd.substring(s.length()).trim();
      initCmdBuilder.add(s);
    }
    ImmutableList<String> initCmd = initCmdBuilder.build();
    // Check that the persistent worker init command matches the action command
    if (!initCmd.equals(argsList.subList(0, initCmd.size()))) {
      throw new IllegalArgumentException("parseInitCmd?![" + initCmd + "]" + "\n" + argsList);
    }
    return initCmd;
  }

  /**
   * Generate a PersistentWorker command + arguments.
   *
   * <p>Strip out all the @____, --flagfile or -flagfile
   *
   * @param args
   * @return a String of command + args, culminating in `--persistent_worker`, used to start a
   *     persistent worker daemon.
   */
  @VisibleForTesting
  static String generatePersistentWorkerCommand(List<String> args) {
    // Strip out the `@...` argfiles.
    List<String> filteredArgs = new ArrayList<>();
    for (String a : args) {
      if (!a.startsWith("@") && !a.startsWith("--flagfile") && !a.startsWith("-flagfile")) {
        filteredArgs.add(a);
      }
    }
    filteredArgs.add(PERSISTENT_WORKER_FLAG);
    return join(" ", filteredArgs);
  }

  private static String getExecutionName(ImmutableList<String> argsList) {
    boolean isScalac = argsList.size() > 1 && argsList.getFirst().endsWith("scalac/scalac");
    if (isScalac) {
      return SCALAC_EXEC_NAME;
    } else if (argsList.contains(JAVABUILDER_JAR)) {
      return JAVAC_EXEC_NAME;
    }
    return "SomeOtherExec";
  }
}
