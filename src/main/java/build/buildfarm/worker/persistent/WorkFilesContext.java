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

  public static WorkFilesContext fromContext(Path opRoot, Tree inputsTree, Command opCommand) {
    return new WorkFilesContext(
        opRoot,
        inputsTree,
        ImmutableList.copyOf(opCommand.getOutputPathsList()),
        ImmutableList.copyOf(opCommand.getOutputFilesList()),
        ImmutableList.copyOf(opCommand.getOutputDirectoriesList()));
  }

  // Paths are absolute paths from the opRoot; same as the Input.getPath();
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
