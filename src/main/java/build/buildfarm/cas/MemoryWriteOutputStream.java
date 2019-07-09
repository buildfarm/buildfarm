package build.buildfarm.cas;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.Write;
import com.google.common.hash.HashingOutputStream;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

class MemoryWriteOutputStream extends OutputStream implements Write {
  private final ContentAddressableStorage storage;
  private final Digest digest;
  private final ListenableFuture<ByteString> writtenFuture;
  private final ByteString.Output out;
  private final SettableFuture<Void> future = SettableFuture.create();
  private HashingOutputStream hashOut;

  MemoryWriteOutputStream(
      ContentAddressableStorage storage,
      Digest digest,
      ListenableFuture<ByteString> writtenFuture) {
    this.storage = storage;
    this.digest = digest;
    this.writtenFuture = writtenFuture;
    if (digest.getSizeBytes() > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          String.format(
              "content size %d exceeds maximum of %d",
              digest.getSizeBytes(),
              Integer.MAX_VALUE));
    }
    out = ByteString.newOutput((int) digest.getSizeBytes());
    hashOut = DigestUtil.forDigest(digest).newHashingOutputStream(out);
    addListener(
        () -> {
          future.set(null);
          try {
            hashOut.close();
          } catch (IOException e) {
            // ignore
          }
        },
        directExecutor());
  }

  String hash() {
    return hashOut.hash().toString();
  }

  Digest getActual() {
    return DigestUtil.buildDigest(hash(), getCommittedSize());
  }

  @Override
  public void close() throws IOException {
    hashOut.close();
    Digest actual = getActual();
    if (!actual.equals(digest)) {
      DigestMismatchException e = new DigestMismatchException(actual, digest);
      future.setException(e);
      throw e;
    }

    try {
      storage.put(
          new ContentAddressableStorage.Blob(out.toByteString(), digest));
    } catch (InterruptedException e) {
      future.setException(e);
      throw new IOException(e);
    }
  }

  @Override
  public void flush() throws IOException {
    hashOut.flush();
  }

  @Override
  public void write(byte[] b) throws IOException {
    hashOut.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    hashOut.write(b, off, len);
  }

  @Override
  public void write(int b) throws IOException {
    hashOut.write(b);
  }

  boolean checkComplete() {
    return writtenFuture.isDone();
  }

  // Write methods

  @Override
  public long getCommittedSize() {
    return isComplete() ? digest.getSizeBytes() : out.size();
  }

  @Override
  public boolean isComplete() {
    return checkComplete();
  }

  @Override
  public OutputStream getOutput() {
    return this;
  }

  @Override
  public void reset() {
    out.reset();
    hashOut = DigestUtil.forDigest(digest).newHashingOutputStream(out);
  }

  @Override
  public void addListener(Runnable onCompleted, Executor executor) {
    writtenFuture.addListener(onCompleted, executor);
  }

  public ListenableFuture<Void> getFuture() {
    return future;
  }
}
