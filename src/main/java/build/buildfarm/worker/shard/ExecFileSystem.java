package build.buildfarm.worker.shard;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.worker.OutputStreamFactory;
import java.nio.file.Path;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

interface ExecFileSystem extends InputStreamFactory, OutputStreamFactory {
  void start(Consumer<List<Digest>> onDigests) throws IOException, InterruptedException;
  void stop();
  ContentAddressableStorage getStorage();
  Path createExecDir(String operationName, Map<Digest, Directory> directoriesIndex, Action action, Command command) throws IOException, InterruptedException;
  void destroyExecDir(Path execDir) throws IOException, InterruptedException;
}
