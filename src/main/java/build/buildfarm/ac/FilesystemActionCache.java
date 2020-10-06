package build.buildfarm.ac;

import static com.google.common.util.concurrent.Futures.immediateFuture;

import build.bazel.remote.execution.v2.ActionResult;
import build.buildfarm.common.DigestUtil.ActionKey;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class FilesystemActionCache implements ActionCache {
  private final Path path;

  public FilesystemActionCache(Path path) {
    this.path = path;
  }

  @Override
  public ListenableFuture<ActionResult> get(ActionKey actionKey) {
    String hash = actionKey.getDigest().getHash();
    Path resultPath = path.resolve(hash);
    try (InputStream in = Files.newInputStream(resultPath)) {
      return immediateFuture(ActionResult.parseFrom(ByteString.readFrom(in)));
    } catch (IOException e) {
      return immediateFuture(null);
    }
  }

  @Override
  public void put(ActionKey actionKey, ActionResult actionResult) throws InterruptedException {
    String hash = actionKey.getDigest().getHash();
    Path resultPath = path.resolve(hash);
    try (OutputStream out = Files.newOutputStream(resultPath)) {
      actionResult.writeTo(out);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
