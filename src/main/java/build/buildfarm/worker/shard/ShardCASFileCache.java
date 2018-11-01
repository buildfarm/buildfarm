package build.buildfarm.worker.shard;

import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.worker.CASFileCache;
import com.google.devtools.remoteexecution.v1test.Digest;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.function.Consumer;

class ShardCASFileCache extends CASFileCache {
  private final InputStreamFactory inputStreamFactory;

  ShardCASFileCache(
      InputStreamFactory shardInputStreamFactory,
      Path root,
      long maxSizeInBytes,
      DigestUtil digestUtil,
      Consumer<Digest> onPut,
      Consumer<Iterable<Digest>> onExpire) {
    super(root, maxSizeInBytes, digestUtil, onPut, onExpire);
    this.inputStreamFactory = createInputStreamFactory(this, shardInputStreamFactory);
  }

  /**
   * this convolution of the use of the CFC as an InputStreamFactory is due to
   * the likelihood that it contains a copy, and possibly the only copy, of a blob,
   * but in an executable state. We resolve first any empty input streams automatically,
   * then attempt to find the blob locally, then fall back to remote download
   *
   * Emprically, this is meant to maintain the fact that we can exist in the only-copy
   * state, with that satisfied by the executable copy, and not cycle.
   */
  private static InputStreamFactory createInputStreamFactory(
      InputStreamFactory primary, InputStreamFactory fallback) {
    return new EmptyInputStreamFactory(
        new FailoverInputStreamFactory(primary, fallback));
  }

  @Override
  protected InputStream newExternalInput(Digest digest, long offset) throws IOException, InterruptedException {
    return inputStreamFactory.newInput(digest, offset);
  }
}
