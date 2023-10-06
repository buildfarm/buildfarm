// Copyright 2019 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.proxy.http;

import static build.buildfarm.common.UrlPath.parseUploadBlobDigest;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.common.RingBufferInputStream;
import build.buildfarm.common.UrlPath.InvalidResourceNameException;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.common.base.Throwables;
import com.google.protobuf.ByteString;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import lombok.extern.java.Log;

@Log
class BlobWriteObserver implements WriteObserver {
  private static final int BLOB_BUFFER_SIZE = 64 * 1024;

  private final String resourceName;
  private final long size;
  private final RingBufferInputStream buffer;
  private final Thread putThread;
  private long committedSize = 0;
  private final AtomicReference<Throwable> error = new AtomicReference<>(null);
  private boolean complete = false;

  BlobWriteObserver(String resourceName, SimpleBlobStore simpleBlobStore)
      throws InvalidResourceNameException {
    Digest digest = parseUploadBlobDigest(resourceName);
    this.resourceName = resourceName;
    this.size = digest.getSizeBytes();
    buffer = new RingBufferInputStream((int) Math.min(size, BLOB_BUFFER_SIZE));
    putThread =
        new Thread(
            () -> {
              try {
                simpleBlobStore.put(digest.getHash(), size, buffer);
              } catch (Exception e) {
                if (!error.compareAndSet(null, e)) {
                  error.get().addSuppressed(e);
                }
                buffer.shutdown();
              }
            });
    putThread.start();
  }

  private void checkError() {
    Throwable t = error.get();
    if (t != null) {
      Throwables.throwIfUnchecked(t);
      throw new RuntimeException(t);
    }
  }

  private void validateRequest(WriteRequest request) {
    checkError();
    String requestResourceName = request.getResourceName();
    if (!requestResourceName.isEmpty() && !resourceName.equals(requestResourceName)) {
      log.log(
          Level.SEVERE,
          String.format(
              "ByteStreamServer:write:%s: resource name %s does not match first request",
              resourceName, requestResourceName));
      throw new IllegalArgumentException(
          String.format(
              "Previous resource name changed while handling request. %s -> %s",
              resourceName, requestResourceName));
    }
    if (complete) {
      log.log(
          Level.SEVERE,
          String.format(
              "ByteStreamServer:write:%s: write received after finish_write specified",
              resourceName));
      throw new IllegalArgumentException("request sent after finish_write request");
    }
    long committedSize = getCommittedSize();
    if (request.getWriteOffset() != committedSize) {
      log.log(
          Level.SEVERE,
          String.format(
              "ByteStreamServer:write:%s: offset %d != committed_size %d",
              resourceName, request.getWriteOffset(), getCommittedSize()));
      throw new IllegalArgumentException("Write offset invalid: " + request.getWriteOffset());
    }
    long sizeAfterWrite = committedSize + request.getData().size();
    if (request.getFinishWrite() && sizeAfterWrite != size) {
      log.log(
          Level.SEVERE,
          String.format(
              "ByteStreamServer:write:%s: finish_write request of size %d for write size %d != expected %d",
              resourceName, request.getData().size(), sizeAfterWrite, size));
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
        complete = true;
        buffer.close();
        putThread.join();
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
    if (!error.compareAndSet(null, t)) {
      error.get().addSuppressed(t);
    }
    buffer.shutdown();
    try {
      putThread.join();
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
