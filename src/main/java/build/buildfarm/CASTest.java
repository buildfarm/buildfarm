package build.buildfarm;

import build.buildfarm.worker.CASFileCache;
import build.buildfarm.common.DigestUtil;
import com.google.devtools.remoteexecution.v1test.Digest;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

class CASTest {
  public static void main(String[] args) throws Exception {
    Path root = Paths.get(args[0]);
    CASFileCache fileCache = new CASFileCache(root, 100l * 1024 * 1024 * 1024, new DigestUtil(DigestUtil.HashFunction.SHA1)) {
      @Override
      protected InputStream newExternalInput(Digest digest, long offset) throws IOException {
        throw new IOException();
      }
    };
    fileCache.start();
    System.out.println("Done with start, ready to roll...");
  }
}
