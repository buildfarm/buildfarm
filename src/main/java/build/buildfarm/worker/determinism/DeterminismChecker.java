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

package build.buildfarm.worker.determinism;

import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.MapUtils;
import build.buildfarm.v1test.QueuedOperation;
import build.buildfarm.worker.OperationContext;
import build.buildfarm.worker.WorkerContext;
import com.google.protobuf.ByteString;
import com.google.rpc.Code;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @class DeterminismChecker
 * @brief Run an action multiple times in order to decide if it is deterministic.
 * @details An action's outputs may not have the same content when run multiple times. Despite the
 *     inputs being the same, and the command invocation the same, different output contents can
 *     still occur because the tool generating the outputs is not deterministic. This can lead to a
 *     poisoned cache, flaky tests, or other unexpected execution results. Sometimes build artifacts
 *     need stamped with a signature and if they are generated differently each time this cannot be
 *     done and it may be difficult to find where in the build process files become tainted. This
 *     checker allows us to detect these problems on a per action basis. This is not done on regular
 *     build/test invocations but is used for debugging and discovering issues.
 */
public class DeterminismChecker {
  private static final Logger logger = Logger.getLogger(DeterminismChecker.class.getName());

  /**
   * @brief Run an action multiple times on behalf of the executor in order to decide if it is
   *     deterministic.
   * @details The success of the action means it was deterministic. This should behave the same way
   *     as normal executions with the extra constraint success requires the action to be
   *     deterministic.
   * @param settings Contextual information needed to run an operation multiple times.
   * @param resultBuilder Determinism results represented as a normal action execution results.
   *     These are built by the check.
   * @return The rpc code to send back to the client. Whether the check fails or succeeds we expect
   *     to send back Code.OK.
   * @note Suggested return identifier: code.
   */
  public static Code checkActionDeterminism(
      DeterminismCheckSettings settings, ActionResult.Builder resultBuilder) {
    // Run the action multiple times to evaluate its determinism.
    DeterminismCheckResults results = getDeterminismResults(settings);

    // build action results so caller knows if action succeeded or failed based on check
    decideActionResults(results, resultBuilder);
    return Code.OK;
  }
  /**
   * @brief Run the determinism test and get back synthesized results.
   * @details This will run the action multiple times, affect the exec filesystem, and compare
   *     output file digests.
   * @param settings Contextual information needed to run an operation multiple times.
   * @return Synthesized results from running a determinism test. Ultimately this says whether the
   *     action was deterministic and provides debug information for if it wasn't.
   * @note Suggested return identifier: determinismResults.
   */
  private static DeterminismCheckResults getDeterminismResults(DeterminismCheckSettings settings) {
    // Run the action once to create a baseline set of output digests.
    runAction(settings.processBuilder);

    // collect digests for future comparison
    List<HashMap<Path, Digest>> fileDigests = new ArrayList<>();
    fileDigests.add(computeFileDigests(settings.workerContext, settings.operationContext));

    // Re-run the action a specified number of times to create comparable output digests.
    for (int i = 0; i < settings.limits.checkDeterminism; ++i) {
      resetWorkingDirectory(settings.workerContext, settings.operationContext);
      runAction(settings.processBuilder);
      fileDigests.add(computeFileDigests(settings.workerContext, settings.operationContext));
    }
    return synthesizeDigestResults(fileDigests);
  }
  /**
   * @brief Reset the action's working directory so the action can be re-run.
   * @details Some actions will behave differently if the original outputs are left in the working
   *     directory. We must give each action run the same initial working directory state.
   * @param workerContext Contextual information for the exec filesystem and configured hashing
   *     behavior.
   * @param operationContext Contextual information about the operation.
   */
  private static void resetWorkingDirectory(
      WorkerContext workerContext, OperationContext operationContext) {
    // After an action runs, the working directory is contaminated with outputs.
    // In order to evaluate another run of the action, we will need to reconstruct the exec
    // filesystem back to the original state.
    // Get information needed to reconstruct the exec filesystem
    try {
      String operationName = operationContext.queueEntry.getExecuteEntry().getOperationName();
      QueuedOperation queuedOperation =
          workerContext.getQueuedOperation(operationContext.queueEntry);
      Map<Digest, Directory> directoriesIndex = createExecDirIndex(workerContext, queuedOperation);

      // Reconstruct the exec filesystem.
      // Create API will remove existing files.

      workerContext.createExecDir(
          operationName,
          directoriesIndex,
          queuedOperation.getAction(),
          queuedOperation.getCommand());
    } catch (IOException | InterruptedException e) {
      logger.log(Level.SEVERE, "cannot create exec dir: ", e);
    }
  }
  /**
   * @brief Create a starting exec directory layout.
   * @details This is required to build the exec filesystem.
   * @param workerContext Contextual information for the exec filesystem and configured hashing
   *     behavior.
   * @param queuedOperation Information about the operation.
   * @note Suggested return identifier: directoryIndex.
   */
  private static Map<Digest, Directory> createExecDirIndex(
      WorkerContext workerContext, QueuedOperation queuedOperation) {
    Map<Digest, Directory> directoriesIndex = new HashMap();
    if (queuedOperation.hasTree()) {
      directoriesIndex =
          DigestUtil.proxyDirectoriesIndex(queuedOperation.getTree().getDirectories());
    } else {
      directoriesIndex =
          workerContext.getDigestUtil().createDirectoriesIndex(queuedOperation.getLegacyTree());
    }
    return directoriesIndex;
  }
  /**
   * @brief Run the action to completion to create it's outputs.
   * @details After the completion of this function we expect the working directory to then contain
   *     all of the outputs mentioned by the action.
   * @param processBuilder How to run the action. Builder should already be ready.
   */
  private static void runAction(ProcessBuilder processBuilder) {
    try {
      Process process = processBuilder.start();
      process.waitFor();
    } catch (IOException | InterruptedException e) {
      logger.log(Level.SEVERE, "process did not complete: ", e);
    }
  }
  /**
   * @brief Assuming the action has completed, calculate all of its output digests.
   * @details These digests will be compared to other action runs.
   * @param workerContext Contextual information for the exec filesystem and configured hashing *
   *     behavior.
   * @param operationContext Contextual information about the operation.
   * @return All of the action's output file digests.
   * @note Suggested return identifier: fileDigests.
   */
  private static HashMap<Path, Digest> computeFileDigests(
      WorkerContext workerContext, OperationContext operationContext) {
    HashMap<Path, Digest> fileDigests = new HashMap<>();

    DigestUtil digestUtil = workerContext.getDigestUtil();

    for (String outputFile : operationContext.command.getOutputFilesList()) {
      Path outputPath = operationContext.execDir.resolve(outputFile);
      addPathDigest(fileDigests, outputPath, digestUtil);
    }
    for (String outputDir : operationContext.command.getOutputDirectoriesList()) {
      Path outputDirPath = operationContext.execDir.resolve(outputDir);
      addDirDigests(fileDigests, outputDirPath, digestUtil);
    }

    return fileDigests;
  }
  /**
   * @brief Compute and store digest of file.
   * @details Adds to digest container.
   * @param fileDigests Container to add digest to.
   * @param path Path to file to compute the digest of.
   * @param digestUtil How to compute the digest.
   */
  private static void addPathDigest(
      HashMap<Path, Digest> fileDigests, Path path, DigestUtil digestUtil) {
    try {
      Digest digest = digestUtil.compute(path);
      fileDigests.put(path, digest);
    } catch (IOException e) {
      logger.log(Level.SEVERE, "could not compute file digest: ", e);
    }
  }
  /**
   * @brief Compute and store digests of directory files.
   * @details Adds to digest container.
   * @param fileDigests Container to add digest to.
   * @param path Path to dir to compute the digest of.
   * @param digestUtil How to compute the digest.
   */
  private static void addDirDigests(
      HashMap<Path, Digest> fileDigests, Path path, DigestUtil digestUtil) {
    try {
      Files.walkFileTree(
          path,
          new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                throws IOException {
              addPathDigest(fileDigests, file, digestUtil);
              return FileVisitResult.CONTINUE;
            }
          });
    } catch (IOException e) {
      logger.log(Level.SEVERE, "could not traverse file tree and collect dir digests: ", e);
    }
  }
  /**
   * @brief Synthesize a map of digests from various action runs to decide determinism results.
   * @details The evaluation will determine if the overall action is deterministic or not. It will
   *     also collect debug information.
   * @param fileDigests File digests from each action run.
   * @return Results that explain whether the overall action is deterministic or not.
   * @note Suggested return identifier: results.
   */
  private static DeterminismCheckResults synthesizeDigestResults(
      List<HashMap<Path, Digest>> fileDigests) {
    // Collect all digest runs into a single map.
    Map<Path, Map<Digest, Integer>> fileDigestCounts = new HashMap();
    for (HashMap<Path, Digest> digestRun : fileDigests) {
      for (Map.Entry<Path, Digest> entry : digestRun.entrySet()) {
        // add file if missing
        fileDigestCounts.putIfAbsent(entry.getKey(), new HashMap());

        // increment it's digest count
        Map<Digest, Integer> updatedDigestCount = fileDigestCounts.get(entry.getKey());
        MapUtils.incrementValue(updatedDigestCount, entry.getValue());
        fileDigestCounts.put(entry.getKey(), updatedDigestCount);
      }
    }

    // If a file has more than 1 digest entry its not deterministic. The reason we count the digest
    // frequency, is to make debugging easier. Debugging messages can contain the digest frequency
    // to determine how nondeterministic file content is.
    DeterminismCheckResults results = new DeterminismCheckResults();
    results.isDeterministic = true;
    for (Map.Entry<Path, Map<Digest, Integer>> entry : fileDigestCounts.entrySet()) {
      // more than 1 digest found for output file
      if (entry.getValue().size() != 1) {
        results.isDeterministic = false;
        results.determinisimFailMessage +=
            entry.getKey() + " has non-deterministic output content:\n";
        results.determinisimFailMessage += MapUtils.toString(entry.getValue()) + "\n";
      }
    }

    return results;
  }
  /**
   * @brief Populate the action builder with results from the determinsim check.
   * @details If the action was not deterministic the results are a failure.
   * @param results The determinism results used to pass or fail the action.
   * @param resultBuilder The action results to build based on the determinism results.
   */
  private static void decideActionResults(
      DeterminismCheckResults results, ActionResult.Builder resultBuilder) {
    // Succeed deterministic actions and fail nondeterministic actions. Failed actions will show
    // which files did not match digests in its stderr. This will help debugging on the client side.
    if (results.isDeterministic) {
      resultBuilder.setExitCode(0);
    } else {
      resultBuilder.setStderrRaw(ByteString.copyFromUtf8(results.determinisimFailMessage));
      resultBuilder.setExitCode(-1);
    }
  }
}
