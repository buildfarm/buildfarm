package build.buildfarm.instance;

import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import java.io.IOException;

@FunctionalInterface
public interface GetDirectoryFunction {
  Directory apply(Digest digest) throws IOException, InterruptedException;
}
