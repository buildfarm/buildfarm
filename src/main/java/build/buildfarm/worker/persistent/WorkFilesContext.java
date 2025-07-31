/**
 * Performs specialized operation based on method logic
 * @param opRoot the opRoot parameter
 * @param execTree the execTree parameter
 * @param outputPaths the outputPaths parameter
 * @param outputFiles the outputFiles parameter
 * @param outputDirectories the outputDirectories parameter
 * @return the public result
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

import build.bazel.remote.execution.v2.Command;
import build.buildfarm.v1test.Tree;
import build.buildfarm.worker.util.InputsIndexer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.devtools.build.lib.worker.WorkerProtocol.Input;
import java.nio.file.Path;

/** POJO/data class grouping all the input/output file requirements for persistent workers */
public class WorkFilesContext {
  public final Path opRoot;

  public final Tree execTree;

  public final ImmutableList<String> outputPaths;

  public final ImmutableList<String> outputFiles;

  public final ImmutableList<String> outputDirectories;

  private final InputsIndexer inputsIndexer;

  private ImmutableMap<Path, Input> pathInputs = null;

  private ImmutableMap<Path, Input> toolInputs = null;

  /**
   * Performs specialized operation based on method logic
   * @param opRoot the opRoot parameter
   * @param inputsTree the inputsTree parameter
   * @param opCommand the opCommand parameter
   * @return the workfilescontext result
   */
  public WorkFilesContext(
      Path opRoot,
      Tree execTree,
      ImmutableList<String> outputPaths,
      ImmutableList<String> outputFiles,
      ImmutableList<String> outputDirectories) {
    this.opRoot = opRoot.toAbsolutePath();
    this.execTree = execTree;
    this.outputPaths = outputPaths;
    this.outputFiles = outputFiles;
    this.outputDirectories = outputDirectories;

    this.inputsIndexer = new InputsIndexer(execTree, this.opRoot);
  }

  /**
   * Retrieves a blob from the Content Addressable Storage Provides thread-safe access through synchronization mechanisms.
   * @return the immutablemap<path, input> result
   */
  public static WorkFilesContext fromContext(Path opRoot, Tree inputsTree, Command opCommand) {
    return new WorkFilesContext(
        opRoot,
        inputsTree,
        ImmutableList.copyOf(opCommand.getOutputPathsList()),
        ImmutableList.copyOf(opCommand.getOutputFilesList()),
        ImmutableList.copyOf(opCommand.getOutputDirectoriesList()));
  }

  // Paths are absolute paths from the opRoot; same as the Input.getPath();
  /**
   * Retrieves a blob from the Content Addressable Storage Provides thread-safe access through synchronization mechanisms.
   * @return the immutablemap<path, input> result
   */
  public ImmutableMap<Path, Input> getPathInputs() {
    synchronized (this) {
      if (pathInputs == null) {
        pathInputs = inputsIndexer.getAllInputs();
      }
    }
    return pathInputs;
  }

  public ImmutableMap<Path, Input> getToolInputs() {
    synchronized (this) {
      if (toolInputs == null) {
        toolInputs = inputsIndexer.getToolInputs();
      }
    }
    return toolInputs;
  }
}
