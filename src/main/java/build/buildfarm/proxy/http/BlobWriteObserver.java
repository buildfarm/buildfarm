package build.buildfarm.proxy.http;

import static build.buildfarm.common.UrlPath.parseUploadBlobDigest;
import static com.google.common.base.Preconditions.checkState;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.common.RingBufferInputStream;
import build.buildfarm.common.UrlPath.InvalidResourceNameException;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.logging.Logger;

class BlobWriteObserver implements WriteObserver {
  private static final Logger logger = Logger.getLogger(BlobWriteObserver.class.getName());
  private static final int BLOB_BUFFER_SIZE = 64 * 1024;

  private final String resourceName;
  private final long size;
  private final RingBufferInputStream buffer;
  private final Thread putThread;
  private long committedSize = 0;
  private boolean complete = false;
  private Throwable error = null;

  BlobWriteObserver(String resourceName, SimpleBlobStore simpleBlobStore) throws InvalidResourceNameException {
    Digest digest = parseUploadBlobDigest(resourceName);
    this.resourceName = resourceName;
    this.size = digest.getSizeBytes();
    buffer = new RingBufferInputStream((int) Math.min(size, BLOB_BUFFER_SIZE));
    putThread = new Thread(() -> {
      try {
        simpleBlobStore.put(digest.getHash(), size, buffer);
      } catch (IOException e) {
        buffer.shutdown();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    putThread.start();
  }

  private void validateRequest(WriteRequest request) {
    checkState(error == null);
    String requestResourceName = request.getResourceName();
    if (!requestResourceName.isEmpty() && !resourceName.equals(requestResourceName)) {
      logger.warning(
          String.format(
              "ByteStreamServer:write:%s: resource name %s does not match first request",
              resourceName,
              requestResourceName));
      throw new IllegalArgumentException(
          String.format(
              "Previous resource name changed while handling request. %s -> %s",
              resourceName,
              requestResourceName));
    }
    if (complete) {
      logger.warning(
          String.format(
              "ByteStreamServer:write:%s: write received after finish_write specified",
              resourceName));
      throw new IllegalArgumentException(
          String.format(
              "request sent after finish_write request"));
    }
    long committedSize = getCommittedSize();
    if (request.getWriteOffset() != committedSize) {
      logger.warning(
          String.format(
              "ByteStreamServer:write:%s: offset %d != committed_size %d",
              resourceName,
              request.getWriteOffset(),
              getCommittedSize()));
      throw new IllegalArgumentException("Write offset invalid: " + request.getWriteOffset());
    }
    long sizeAfterWrite = committedSize + request.getData().size();
    if (request.getFinishWrite() && sizeAfterWrite != size) {
      logger.warning(
          String.format(
              "ByteStreamServer:write:%s: finish_write request of size %d for write size %d != expected %d",
              resourceName,
              request.getData().size(),
              sizeAfterWrite,
              size));
      throw new IllegalArgumentException("Write size invalid: " + sizeAfterWrite);
    }
  }

  @Override
  public void onNext(WriteRequest request) {
    boolean shutdownBuffer = true;
    try {
      validateRequest(request);
      ByteString data = request.getData();
      buffer.write(data.toByteArray());
      committedSize += data.size();
      shutdownBuffer = false;
      if (request.getFinishWrite()) {
        putThread.join();
        complete = true;
      }
    } catch (InterruptedException e) {
      // prevent buffer mitigation
      shutdownBuffer = false;
      Thread.currentThread().interrupt();
    } finally {
      if (shutdownBuffer) {
        buffer.shutdown();
      }
    }
  }

  @Override
  public void onError(Throwable t) {
    buffer.shutdown();
    try {
      putThread.join();
      error = t;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void onCompleted() {
    try {
      putThread.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public long getCommittedSize() {
    return committedSize;
  }

  @Override
  public boolean getComplete() {
    return complete;
  }
}
