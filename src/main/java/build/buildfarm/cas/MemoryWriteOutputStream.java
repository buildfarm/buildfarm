/**
 * Performs specialized operation based on method logic
 * @return the string result
 */
/**
 * Performs specialized operation based on method logic Includes input validation and error handling for robustness.
 */
// Copyright 2019 The Buildfarm Authors. All rights reserved.
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

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.Write;
import build.buildfarm.common.io.FeedbackOutputStream;
import build.buildfarm.v1test.Digest;
import com.google.common.hash.HashingOutputStream;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

class MemoryWriteOutputStream extends FeedbackOutputStream implements Write {
  private final ContentAddressableStorage storage;
  private final DigestUtil digestUtil;
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
    if (digest.getSize() > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          String.format(
              "content size %d exceeds maximum of %d", digest.getSize(), Integer.MAX_VALUE));
    }
    out = ByteString.newOutput((int) digest.getSize());
    digestUtil = new DigestUtil(HashFunction.get(digest.getDigestFunction()));
    hashOut = digestUtil.newHashingOutputStream(out);
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
    return digestUtil.build(hash(), getCommittedSize());
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   */
  public void close() throws IOException {
    if (getCommittedSize() >= digest.getSize()) {
      hashOut.close();
      closedFuture.set(null);
      Digest actual = getActual();
      if (!actual.equals(digest)) {
        DigestMismatchException e = new DigestMismatchException(actual, digest);
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
  /**
   * Persists data to storage or external destination
   * @param b the b parameter
   */
  public void flush() throws IOException {
    hashOut.flush();
  }

  @Override
  /**
   * Persists data to storage or external destination
   * @param b the b parameter
   * @param off the off parameter
   * @param len the len parameter
   */
  public void write(byte[] b) throws IOException {
    hashOut.write(b);
  }

  @Override
  /**
   * Persists data to storage or external destination
   * @param b the b parameter
   */
  public void write(byte[] b, int off, int len) throws IOException {
    hashOut.write(b, off, len);
  }

  @Override
  /**
   * Loads data from storage or external source
   * @return the boolean result
   */
  public void write(int b) throws IOException {
    hashOut.write(b);
  }

  @Override
  public boolean isReady() {
    return true;
  }

  // Write methods

  @Override
  /**
   * Performs specialized operation based on method logic
   * @return the boolean result
   */
  public long getCommittedSize() {
    return isComplete() ? digest.getSize() : out.size();
  }

  @Override
  /**
   * Retrieves a blob from the Content Addressable Storage Executes asynchronously and returns a future for completion tracking. Includes input validation and error handling for robustness.
   * @param offset the offset parameter
   * @param deadlineAfter the deadlineAfter parameter
   * @param deadlineAfterUnits the deadlineAfterUnits parameter
   * @param onReadyHandler the onReadyHandler parameter
   * @return the feedbackoutputstream result
   */
  public boolean isComplete() {
    return writtenFuture.isDone();
  }

  @Override
  /**
   * Retrieves a blob from the Content Addressable Storage
   * @param offset the offset parameter
   * @param deadlineAfter the deadlineAfter parameter
   * @param deadlineAfterUnits the deadlineAfterUnits parameter
   * @param onReadyHandler the onReadyHandler parameter
   * @return the listenablefuture<feedbackoutputstream> result
   */
  public synchronized FeedbackOutputStream getOutput(
      long offset, long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler) {
    if (closedFuture == null || closedFuture.isDone()) {
      closedFuture = SettableFuture.create();
    }
    checkState(offset == 0, "cannot position MemoryWriteOutputStream");
    return this;
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   */
  public synchronized ListenableFuture<FeedbackOutputStream> getOutputFuture(
      long offset, long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler) {
    if (closedFuture == null || closedFuture.isDone()) {
      return immediateFuture(getOutput(offset, deadlineAfter, deadlineAfterUnits, onReadyHandler));
    }
    return transformAsync(
        closedFuture,
        result -> getOutputFuture(offset, deadlineAfter, deadlineAfterUnits, onReadyHandler),
        directExecutor());
  }

  @Override
  public void reset() {
    out.reset();
    hashOut = digestUtil.newHashingOutputStream(out);
  }

  @Override
  public ListenableFuture<Long> getFuture() {
    return Futures.transform(future, result -> digest.getSize(), directExecutor());
  }
}
