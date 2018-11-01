package build.buildfarm.worker.shard;

import build.buildfarm.common.ContentAddressableStorage;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.worker.OutputStreamFactory;
import com.google.devtools.remoteexecution.v1test.Action;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.Directory;
import java.nio.file.Path;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

interface ExecFileSystem extends InputStreamFactory, OutputStreamFactory {
  void start(Consumer<List<Digest>> onDigests) throws IOException, InterruptedException;
  void stop();
  ContentAddressableStorage getStorage();
  Path createExecDir(String operationName, Map<Digest, Directory> directoriesIndex, Action action) throws IOException, InterruptedException;
  void destroyExecDir(Path execDir) throws IOException, InterruptedException;
}
