// Copyright 2023 The Bazel Authors. All rights reserved.
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

package build.buildfarm.worker.util;

import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.NodeProperty;
import build.buildfarm.common.ProxyDirectoriesIndex;
import build.buildfarm.v1test.Tree;
import com.google.common.collect.ImmutableMap;
import com.google.devtools.build.lib.worker.WorkerProtocol.Input;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.Map;

/**
 * Organizes action Inputs into files, extracting their paths, and differentiates tool inputs (e.g.
 * JavaBuilder, Scalac, etc.)
 *
 * <p>Indexes (and partitions) Inputs from an action's Merkle Tree.
 */
public class InputsIndexer {
  // See: https://github.com/bazelbuild/bazel/issues/10091
  public static final String BAZEL_TOOL_INPUT_MARKER = "bazel_tool_input";

  final Tree tree;
  final Map<Digest, Directory> proxyDirs;

  final FileSystem fs;

  final Path opRoot;

  ImmutableMap<Path, FileNode> files = null;
  ImmutableMap<Path, Input> absPathInputs = null;
  ImmutableMap<Path, Input> toolInputs = null;

  public InputsIndexer(Tree tree, Path opRoot) {
    this.tree = tree;
    this.proxyDirs = new ProxyDirectoriesIndex(tree.getDirectoriesMap());
    this.opRoot = opRoot;
    this.fs = opRoot.getFileSystem();
  }

  // https://stackoverflow.com/questions/22611919/why-do-i-get-providermismatchexception-when-i-try-to-relativize-a-path-agains
  public Path pathTransform(final Path path) {
    Path ret = fs.getPath(path.isAbsolute() ? fs.getSeparator() : "");
    for (final Path component : path) ret = ret.resolve(component.getFileName().toString());
    return ret;
  }

  public ImmutableMap<Path, Input> getAllInputs() {
    if (absPathInputs == null) {
      ImmutableMap<Path, FileNode> relFiles = getAllFiles();
      ImmutableMap.Builder<Path, Input> inputs = ImmutableMap.builder();

      for (Map.Entry<Path, FileNode> pf : relFiles.entrySet()) {
        Path absPath = this.opRoot.resolve(pf.getKey()).normalize();
        inputs.put(absPath, inputFromFile(absPath, pf.getValue()));
      }
      absPathInputs = inputs.build();
    }
    return absPathInputs;
  }

  public ImmutableMap<Path, Input> getToolInputs() {
    if (toolInputs == null) {
      ImmutableMap<Path, FileNode> relFiles = getAllFiles();
      ImmutableMap.Builder<Path, Input> inputs = ImmutableMap.builder();

      for (Map.Entry<Path, FileNode> pf : relFiles.entrySet()) {
        FileNode fn = pf.getValue();
        if (isToolInput(fn)) {
          Path absPath = this.opRoot.resolve(pf.getKey());
          inputs.put(absPath, inputFromFile(absPath, fn));
        }
      }
      toolInputs = inputs.build();
    }
    return toolInputs;
  }

  private ImmutableMap<Path, FileNode> getAllFiles() {
    if (files == null) {
      ImmutableMap.Builder<Path, FileNode> accumulator = ImmutableMap.builder();
      Directory rootDir = proxyDirs.get(tree.getRootDigest());

      Path fsRelative = fs.getPath(".");
      files = getFilesFromDir(fsRelative, rootDir, accumulator).build();
    }
    return files;
  }

  private Input inputFromFile(Path absPath, FileNode fileNode) {
    return Input.newBuilder()
        .setPath(absPath.toString())
        .setDigest(fileNode.getDigest().getHashBytes())
        .build();
  }

  private ImmutableMap.Builder<Path, FileNode> getFilesFromDir(
      Path dirPath, Directory dir, ImmutableMap.Builder<Path, FileNode> acc) {
    dir.getFilesList()
        .forEach(
            fileNode -> {
              Path path = dirPath.resolve(fileNode.getName()).normalize();
              acc.put(path, fileNode);
            });

    // Recurse into subdirectories
    dir.getDirectoriesList()
        .forEach(
            dirNode ->
                getFilesFromDir(
                    dirPath.resolve(dirNode.getName()),
                    this.proxyDirs.get(dirNode.getDigest()),
                    acc));
    return acc;
  }

  private static boolean isToolInput(FileNode fileNode) {
    for (NodeProperty prop : fileNode.getNodeProperties().getPropertiesList()) {
      if (prop.getName().equals(BAZEL_TOOL_INPUT_MARKER)) {
        return true;
      }
    }
    return false;
  }
}
