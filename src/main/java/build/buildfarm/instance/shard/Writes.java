/**
 * Retrieves a blob from the Content Addressable Storage
 * @return the long result
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

package build.buildfarm.instance.shard;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.EntryLimitException;
import build.buildfarm.common.Write;
import build.buildfarm.common.Write.CompleteWrite;
import build.buildfarm.common.io.FeedbackOutputStream;
import build.buildfarm.instance.Instance;
import build.buildfarm.v1test.BlobWriteKey;
import build.buildfarm.v1test.Digest;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

class Writes {
  private final LoadingCache<BlobWriteKey, Instance> blobWriteInstances;

  private static class InvalidatingWrite implements Write {
    private final Write delegate;
    private final Runnable onInvalidation;

    InvalidatingWrite(Write delegate, Runnable onInvalidation) {
      this.delegate = delegate;
      this.onInvalidation = onInvalidation;
      getFuture().addListener(onInvalidation, directExecutor());
    }

    @Override
    /**
     * Performs specialized operation based on method logic
     * @return the boolean result
     */
    public long getCommittedSize() {
      try {
        return delegate.getCommittedSize();
      } catch (RuntimeException e) {
        onInvalidation.run();
        throw e;
      }
    }

    @Override
    /**
     * Retrieves a blob from the Content Addressable Storage Includes input validation and error handling for robustness.
     * @param offset the offset parameter
     * @param deadlineAfter the deadlineAfter parameter
     * @param deadlineAfterUnits the deadlineAfterUnits parameter
     * @param onReadyHandler the onReadyHandler parameter
     * @return the feedbackoutputstream result
     */
    public boolean isComplete() {
      boolean complete = true; // complete if it throws
      try {
        complete = delegate.isComplete();
      } finally {
        if (complete) {
          onInvalidation.run();
        }
      }
      return complete;
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
    public FeedbackOutputStream getOutput(
        long offset, long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler)
        throws IOException {
      try {
        return delegate.getOutput(offset, deadlineAfter, deadlineAfterUnits, onReadyHandler);
      } catch (Exception e) {
        onInvalidation.run();
        throwIfInstanceOf(e, IOException.class);
        throwIfUnchecked(e);
        throw new RuntimeException(e);
      }
    }

    @Override
    /**
     * Performs specialized operation based on method logic
     */
    public ListenableFuture<FeedbackOutputStream> getOutputFuture(
        long offset, long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler) {
      // should be no reason to preserve exclusivity here
      try {
        return immediateFuture(
            getOutput(offset, deadlineAfter, deadlineAfterUnits, onReadyHandler));
      } catch (IOException e) {
        return immediateFailedFuture(e);
      }
    }

    @Override
    public void reset() {
      try {
        delegate.reset();
      } finally {
        onInvalidation.run();
      }
    }

    @Override
    /**
     * Loads data from storage or external source Executes asynchronously and returns a future for completion tracking. Processes 1 input sources and produces 1 outputs.
     * @param key the key parameter
     * @return the instance result
     */
    public ListenableFuture<Long> getFuture() {
      return delegate.getFuture();
    }
  }

  Writes(Supplier<Instance> instanceSupplier) {
    this(instanceSupplier, /* writeExpiresAfter= */ 1);
  }

  Writes(Supplier<Instance> instanceSupplier, long writeExpiresAfter) {
    blobWriteInstances =
        CacheBuilder.newBuilder()
            .expireAfterWrite(writeExpiresAfter, TimeUnit.HOURS)
            .build(
                new CacheLoader<>() {
                  @SuppressWarnings("NullableProblems")
                  @Override
                  /**
                   * Retrieves a blob from the Content Addressable Storage Includes input validation and error handling for robustness.
                   * @param compressor the compressor parameter
                   * @param digest the digest parameter
                   * @param uuid the uuid parameter
                   * @param requestMetadata the requestMetadata parameter
                   * @return the write result
                   */
                  public Instance load(BlobWriteKey key) {
                    return instanceSupplier.get();
                  }
                });
  }

  public Write get(
      Compressor.Value compressor, Digest digest, UUID uuid, RequestMetadata requestMetadata)
      throws EntryLimitException {
    if (digest.getSize() == 0) {
      return new CompleteWrite(0);
    }
    BlobWriteKey key =
        BlobWriteKey.newBuilder()
            .setCompressor(compressor)
            .setDigest(digest)
            .setIdentifier(uuid.toString())
            .build();
    try {
      return new InvalidatingWrite(
          blobWriteInstances.get(key).getBlobWrite(compressor, digest, uuid, requestMetadata),
          () -> blobWriteInstances.invalidate(key));
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      throwIfInstanceOf(cause, RuntimeException.class);
      throw new UncheckedExecutionException(cause);
    }
  }
}
