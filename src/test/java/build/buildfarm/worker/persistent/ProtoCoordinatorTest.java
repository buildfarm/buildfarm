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

import static com.google.common.truth.Truth.assertThat;

import build.bazel.remote.execution.v2.Command;
import build.buildfarm.common.StubClaimAcquirer;
import build.buildfarm.v1test.Tree;
import build.buildfarm.worker.util.WorkerTestUtils;
import build.buildfarm.worker.util.WorkerTestUtils.TreeFile;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import persistent.bazel.client.PersistentWorker;
import persistent.bazel.client.WorkerKey;

@RunWith(JUnit4.class)
public class ProtoCoordinatorTest {
  private WorkerKey makeWorkerKey(
      WorkFilesContext ctx, WorkerInputs workerFiles, Path workRootsDir) {
    return Keymaker.make(
        ctx.opRoot,
        workRootsDir,
        ImmutableList.of("workerExecCmd"),
        ImmutableList.of("workerInitArgs"),
        ImmutableMap.of(),
        "executionName",
        workerFiles);
  }

  private Path rootDir = null;

  public Path jimFsRoot() {
    if (rootDir == null) {
      rootDir =
          Iterables.getFirst(
              Jimfs.newFileSystem(
                      Configuration.unix().toBuilder()
                          .setAttributeViews("basic", "owner", "posix", "unix")
                          .build())
                  .getRootDirectories(),
              null);
    }
    return rootDir;
  }

  @Test
  public void testProtoCoordinator() throws Exception {
    ProtoCoordinator pc = ProtoCoordinator.ofCommonsPool(4, new StubClaimAcquirer());

    Path fsRoot = jimFsRoot();
    Path opRoot = fsRoot.resolve("opRoot");
    assertThat(Files.notExists(opRoot)).isTrue();
    Files.createDirectory(opRoot);

    assertThat(Files.exists(opRoot)).isTrue();

    String treeRootDir = opRoot.toString();
    List<TreeFile> fileInputs =
        ImmutableList.of(
            new TreeFile("file_1", "file contents 1"),
            new TreeFile("subdir/subdir_file_2", "file contents 2"),
            new TreeFile("tools_dir/tool_file", "tool file contents", true),
            new TreeFile("tools_dir/tool_file_2", "tool file contents 2", true));

    Tree tree = WorkerTestUtils.makeTree(treeRootDir, fileInputs);

    Command command = WorkerTestUtils.makeCommand();
    WorkFilesContext ctx = WorkFilesContext.fromContext(opRoot, tree, command);
    ImmutableList<String> requestArgs = ImmutableList.of("reqArg1");

    WorkerInputs workerFiles = WorkerInputs.from(ctx, requestArgs);

    for (Path file : workerFiles.allInputs.keySet()) {
      Files.createDirectories(file.getParent());
      Files.createFile(file);
    }

    WorkerKey key = makeWorkerKey(ctx, workerFiles, fsRoot.resolve("workRootsDir"));

    Path workRoot = key.getExecRoot();
    Path toolsRoot = workRoot.resolve(PersistentWorker.TOOL_INPUT_SUBDIR);

    // Assert: all Tools are copied into "/workRootsDir/*/tool_inputs"
    assertThat(toolsRoot.toString()).startsWith(workRoot.toString());
    pc.copyToolInputsIntoWorkerToolRoot(key, workerFiles);

    assertThat(Files.exists(workRoot)).isTrue();
    assertThat(Files.exists(toolsRoot)).isTrue();
    Set<Path> expectedToolInputs = new HashSet<>();
    for (TreeFile file : fileInputs) {
      if (file.isTool) {
        expectedToolInputs.add(toolsRoot.resolve(file.path));
      }
    }
    assertThat(WorkerTestUtils.listFilesRec(workRoot))
        .containsExactlyElementsIn(expectedToolInputs);

    List<Path> expectedOpRootFiles = new ArrayList<>();
    // Create some fake output files.
    for (String pathStr : ctx.outputFiles) {
      Path file = workRoot.resolve(pathStr);
      Files.createDirectories(file.getParent());
      Files.createFile(file);
      expectedOpRootFiles.add(opRoot.resolve(pathStr));
    }
    pc.moveOutputsToOperationRoot(ctx, workRoot);

    assertThat(WorkerTestUtils.listFilesRec(opRoot)).containsAtLeastElementsIn(expectedOpRootFiles);
    // At this point, the only thing left in the `workRoot` should be the tools.
    List<Path> workRootPaths = WorkerTestUtils.listFilesRec(workRoot);
    assertThat(workRootPaths).containsAtLeastElementsIn(expectedToolInputs);
    assertThat(workRootPaths).containsNoneIn(expectedOpRootFiles);
  }
}
