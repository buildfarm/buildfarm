package build.buildfarm;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.worker.CASFileCache;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;

class CASTest {
  private static class LocalCASFileCache extends CASFileCache {
    LocalCASFileCache(
        Path root,
        long maxSizeInBytes,
        DigestUtil digestUtil,
        ExecutorService expireService) {
      super(root, maxSizeInBytes, digestUtil, expireService);
    }

    @Override
    protected InputStream newExternalInput(Digest digest, long offset) throws IOException {
      throw new IOException();
    }
  }

  public static void main(String[] args) throws Exception {
    Path root = Paths.get(args[0]);
    CASFileCache fileCache = new LocalCASFileCache(
        root,
        /* maxSizeInBytes=*/ 100l * 1024 * 1024 * 1024,
        new DigestUtil(HashFunction.SHA1),
        /* expireService=*/ newDirectExecutorService());
    fileCache.start(newDirectExecutorService());
    System.out.println("Done with start, ready to roll...");
  }
}
