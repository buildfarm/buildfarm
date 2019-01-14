package build.buildfarm.worker.operationqueue;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.worker.CASFileCache;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

class InjectedCASFileCache extends CASFileCache {
  private final InputStreamFactory inputStreamFactory;

  InjectedCASFileCache(
      InputStreamFactory inputStreamFactory,
      Path root,
      long maxSizeInBytes,
      DigestUtil digestUtil,
      ExecutorService expireService) {
    super(root, maxSizeInBytes, digestUtil, expireService);
    this.inputStreamFactory = inputStreamFactory;
  }

  @Override
  protected InputStream newExternalInput(Digest digest, long offset) throws IOException, InterruptedException {
    return inputStreamFactory.newInput(digest, offset);
  }
}
