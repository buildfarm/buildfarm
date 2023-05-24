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

package build.buildfarm.cas;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.Write;
import build.buildfarm.common.io.FeedbackOutputStream;
import com.google.common.hash.HashingOutputStream;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

class MemoryWriteOutputStream extends FeedbackOutputStream implements Write {
  private final ContentAddressableStorage storage;
  private final Digest digest;
  private final ListenableFuture<ByteString> writtenFuture;
  private final ByteString.Output out;
  private final SettableFuture<Void> future = SettableFuture.create();
  private SettableFuture<Void> closedFuture = null;
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
              "content size %d exceeds maximum of %d", digest.getSizeBytes(), Integer.MAX_VALUE));
    }
    out = ByteString.newOutput((int) digest.getSizeBytes());
    hashOut = DigestUtil.forDigest(digest).newHashingOutputStream(out);
    writtenFuture.addListener(
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
    if (getCommittedSize() >= digest.getSizeBytes()) {
      hashOut.close();
      closedFuture.set(null);
      Digest actual = getActual();
      if (!actual.equals(digest)) {
        DigestMismatchException e = new DigestMismatchException(actual, digest, "MemoryWriteOutputStream.close");
        future.setException(e);
        throw e;
      }

      try {
        storage.put(new ContentAddressableStorage.Blob(out.toByteString(), digest));
      } catch (InterruptedException e) {
        future.setException(e);
        throw new IOException(e);
      }
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

  @Override
  public boolean isReady() {
    return true;
  }

  // Write methods

  @Override
  public long getCommittedSize() {
    return isComplete() ? digest.getSizeBytes() : out.size();
  }

  @Override
  public boolean isComplete() {
    return writtenFuture.isDone();
  }

  @Override
  public synchronized FeedbackOutputStream getOutput(
      long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler) {
    if (closedFuture == null || closedFuture.isDone()) {
      closedFuture = SettableFuture.create();
    }
    return this;
  }

  @Override
  public synchronized ListenableFuture<FeedbackOutputStream> getOutputFuture(
      long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler) {
    if (closedFuture == null || closedFuture.isDone()) {
      return immediateFuture(getOutput(deadlineAfter, deadlineAfterUnits, onReadyHandler));
    }
    return transformAsync(
        closedFuture,
        result -> getOutputFuture(deadlineAfter, deadlineAfterUnits, onReadyHandler),
        directExecutor());
  }

  @Override
  public void reset() {
    out.reset();
    hashOut = DigestUtil.forDigest(digest).newHashingOutputStream(out);
  }

  @Override
  public ListenableFuture<Long> getFuture() {
    return Futures.transform(future, result -> digest.getSizeBytes(), directExecutor());
  }
}
