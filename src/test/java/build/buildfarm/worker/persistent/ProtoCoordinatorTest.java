package build.buildfarm.worker.persistent;

import build.bazel.remote.execution.v2.Command;
import build.buildfarm.v1test.Tree;
import build.buildfarm.worker.util.WorkerTestUtils;
import build.buildfarm.worker.util.WorkerTestUtils.TreeFile;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.google.devtools.build.lib.worker.WorkerProtocol.Input;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
                      Configuration.unix()
                          .toBuilder()
                          .setAttributeViews("basic", "owner", "posix", "unix")
                          .build())
                  .getRootDirectories(),
              null);
    }
    return rootDir;
  }

  @Test
  public void testProtoCoordinator() throws Exception {
    ProtoCoordinator pc = ProtoCoordinator.ofCommonsPool(4);

    Path fsRoot = jimFsRoot();
    Path opRoot = fsRoot.resolve("opRoot");
    assert (Files.notExists(opRoot));
    Files.createDirectory(opRoot);

    assert (Files.exists(opRoot));

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

    for (Map.Entry<Path, Input> entry : workerFiles.allInputs.entrySet()) {
      Path file = entry.getKey();
      Files.createDirectories(file.getParent());
      Files.createFile(file);
    }

    WorkerKey key = makeWorkerKey(ctx, workerFiles, fsRoot.resolve("workRootsDir"));

    Path workRoot = key.getExecRoot();
    Path toolsRoot = workRoot.resolve(PersistentWorker.TOOL_INPUT_SUBDIR);

    pc.copyToolInputsIntoWorkerToolRoot(key, workerFiles);

    assert Files.exists(workRoot);
    List<Path> expectedToolInputs = new ArrayList<>();
    for (TreeFile file : fileInputs) {
      if (file.isTool) {
        expectedToolInputs.add(toolsRoot.resolve(file.path));
      }
    }
    WorkerTestUtils.assertFilesExistExactly(workRoot, expectedToolInputs);

    List<Path> expectedOpRootFiles = new ArrayList<>();

    // Check that we move specified output files (assuming they exist)
    for (String pathStr : ctx.outputFiles) {
      Path file = workRoot.resolve(pathStr);
      Files.createDirectories(file.getParent());
      Files.createFile(file);
      expectedOpRootFiles.add(opRoot.resolve(pathStr));
    }

    pc.moveOutputsToOperationRoot(ctx, workRoot);

    WorkerTestUtils.assertFilesExistExactly(opRoot, expectedOpRootFiles);
  }
}
