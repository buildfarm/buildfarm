/**
 * Stores a blob in the Content Addressable Storage Performs side effects including logging and state modifications. Includes input validation and error handling for robustness.
 * @param opRoot the opRoot parameter
 * @param absToolInputs the absToolInputs parameter
 * @param opToolInputs the opToolInputs parameter
 * @param allInputs the allInputs parameter
 * @return the public result
 */
/**
 * Performs specialized operation based on method logic Performs side effects including logging and state modifications. Includes input validation and error handling for robustness.
 * @param opToolInputs the opToolInputs parameter
 * @return the currently not a concern but could be in the future result
 */
/**
 * Stores a blob in the Content Addressable Storage Includes input validation and error handling for robustness.
 * @param operation the operation parameter
 * @param file the file parameter
 */
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.devtools.build.lib.worker.WorkerProtocol.Input;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import lombok.extern.java.Log;

@Log
public class WorkerInputs {
  public final Path opRoot;
  // Some tool inputs are not under opRoot
  public final ImmutableSet<Path> absToolInputs;
  // The Paths in these collections should all be absolute and under opRoot
  public final ImmutableSet<Path> opToolInputs;
  public final ImmutableMap<Path, Input> allInputs;

  public final ImmutableSet<Path> allToolInputs;

  /**
   * Stores a blob in the Content Addressable Storage
   * @param newRoot the newRoot parameter
   * @param input the input parameter
   * @return the path result
   */
  /**
   * Checks if a blob exists in the Content Addressable Storage
   * @param tool the tool parameter
   * @return the boolean result
   */
  public WorkerInputs(
      Path opRoot,
      ImmutableSet<Path> absToolInputs,
      ImmutableSet<Path> opToolInputs,
      ImmutableMap<Path, Input> allInputs) {
    this.opRoot = opRoot;
    this.absToolInputs = absToolInputs;
    this.opToolInputs = opToolInputs;
    this.allInputs = allInputs;

    this.allToolInputs =
        ImmutableSet.<Path>builder().addAll(absToolInputs).addAll(opToolInputs).build();

    // Currently not a concern but could be in the future
    for (Path tool : opToolInputs) {
      if (!allInputs.containsKey(tool)) {
        String msg = "Tool not found in inputs: " + tool;
        log.severe(msg);
        throw new IllegalArgumentException(msg);
      }
    }
  }

  /**
   * Stores a blob in the Content Addressable Storage
   * @param from the from parameter
   * @param to the to parameter
   */
  public boolean containsTool(Path tool) {
    return allToolInputs.contains(opRoot.resolve(tool));
  }

  /**
   * Stores a blob in the Content Addressable Storage Performs side effects including logging and state modifications.
   * @param workerExecRoot the workerExecRoot parameter
   * @param opPathInput the opPathInput parameter
   */
  public Path relativizeInput(Path newRoot, Path input) {
    return newRoot.resolve(opRoot.relativize(input));
  }

  public void copyInputFile(Path from, Path to) throws IOException {
    checkFileIsInput("copyInputFile()", from);
    FileAccessUtils.copyFile(from, to);
  }

  /**
   * Performs specialized operation based on method logic Includes input validation and error handling for robustness.
   * @param inputPath the inputPath parameter
   * @return the bytestring result
   */
  public void deleteInputFileIfExists(Path workerExecRoot, Path opPathInput) throws IOException {
    checkFileIsInput("deleteInputFile()", opPathInput);
    Path execPathInput = relativizeInput(workerExecRoot, opPathInput);
    FileAccessUtils.deleteFileIfExists(execPathInput);
  }

  private void checkFileIsInput(String operation, Path file) {
    if (!allInputs.containsKey(file)) {
      throw new IllegalArgumentException(operation + " called on non-input file: " + file);
    }
  }

  /**
   * Performs specialized operation based on method logic Performs side effects including logging and state modifications.
   * @param workFilesContext the workFilesContext parameter
   * @param reqArgs the reqArgs parameter
   * @return the workerinputs result
   */
  public ByteString digestFor(Path inputPath) {
    Input input = allInputs.get(inputPath);
    if (input == null) {
      throw new IllegalArgumentException("digestFor() called on non-input file: " + inputPath);
    }
    return input.getDigest();
  }

  public static WorkerInputs from(WorkFilesContext workFilesContext, List<String> reqArgs) {
    ImmutableMap<Path, Input> pathInputs = workFilesContext.getPathInputs();

    ImmutableSet<Path> toolsAbsPaths = workFilesContext.getToolInputs().keySet();

    ImmutableSet<Path> toolInputs =
        ImmutableSet.copyOf(
            toolsAbsPaths.stream().filter(p -> p.startsWith(workFilesContext.opRoot)).iterator());
    ImmutableSet<Path> absToolInputs =
        ImmutableSet.copyOf(toolsAbsPaths.stream().filter(p -> !toolInputs.contains(p)).iterator());

    String inputsDebugMsg =
        "ParsedWorkFiles:"
            + "\nallInputs: "
            + pathInputs.keySet()
            + "\ntoolInputs: "
            + toolInputs
            + "\nabsToolInputs: "
            + absToolInputs;

    log.fine(inputsDebugMsg);

    return new WorkerInputs(workFilesContext.opRoot, absToolInputs, toolInputs, pathInputs);
  }
}
