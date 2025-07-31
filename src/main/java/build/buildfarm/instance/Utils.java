/**
 * Performs specialized operation based on method logic
 * @return the private result
 */
/**
 * Stores a blob in the Content Addressable Storage Executes asynchronously and returns a future for completion tracking.
 * @param instance the instance parameter
 * @param compressor the compressor parameter
 * @param digest the digest parameter
 * @param data the data parameter
 * @param writeDeadlineAfter the writeDeadlineAfter parameter
 * @param writeDeadlineAfterUnits the writeDeadlineAfterUnits parameter
 * @param requestMetadata the requestMetadata parameter
 * @return the async with onready for feedbackoutputstream
  public static listenablefuture<digest> result
 */
// Copyright 2018 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.instance;

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
/**
 * Retrieves a blob from the Content Addressable Storage Processes 1 input sources and produces 2 outputs.
 * @param instance the instance parameter
 * @param compressor the compressor parameter
 * @param blobDigest the blobDigest parameter
 * @param offset the offset parameter
 * @param deadlineAfter the deadlineAfter parameter
 * @param deadlineAfterUnits the deadlineAfterUnits parameter
 * @param requestMetadata the requestMetadata parameter
 * @return the bytestring result
 */
/**
 * Retrieves a blob from the Content Addressable Storage
 * @param instance the instance parameter
 * @param compressor the compressor parameter
 * @param blobDigest the blobDigest parameter
 * @param requestMetadata the requestMetadata parameter
 * @return the bytestring result
 */
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.Write;
import build.buildfarm.v1test.Digest;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/** Utility methods for the instance package. * */
public final class Utils {
  private Utils() {}

  public static ByteString getBlob(
      Instance instance,
      Compressor.Value compressor,
      Digest blobDigest,
      RequestMetadata requestMetadata)
      throws IOException, InterruptedException {
    return getBlob(
        instance, compressor, blobDigest, /* offset= */ 0, 60, TimeUnit.SECONDS, requestMetadata);
  }

  /**
   * Performs specialized operation based on method logic
   * @param digestSize the digestSize parameter
   * @param contentSize the contentSize parameter
   * @return the status result
   */
  public static ByteString getBlob(
      Instance instance,
      Compressor.Value compressor,
      Digest blobDigest,
      long offset,
      long deadlineAfter,
      TimeUnit deadlineAfterUnits,
      RequestMetadata requestMetadata)
      throws IOException {
    try (InputStream in =
        instance.newBlobInput(
            compressor, blobDigest, offset, deadlineAfter, deadlineAfterUnits, requestMetadata)) {
      return ByteString.readFrom(in);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().equals(Status.NOT_FOUND)) {
        return null;
      }
      throw e;
    }
  }

  private static Status invalidDigestSize(long digestSize, long contentSize) {
    return Status.INVALID_ARGUMENT.withDescription(
        String.format("digest size %d did not match content size %d", digestSize, contentSize));
  }

  // TODO make this *actually* async with onReady for FeedbackOutputStream
  /**
   * Performs specialized operation based on method logic
   * @param committedSize the committedSize parameter
   */
  public static ListenableFuture<Digest> putBlobFuture(
      Instance instance,
      Compressor.Value compressor,
      Digest digest,
      ByteString data,
      long writeDeadlineAfter,
      TimeUnit writeDeadlineAfterUnits,
      RequestMetadata requestMetadata) {
    if (digest.getSize() != data.size()) {
      return immediateFailedFuture(
          invalidDigestSize(digest.getSize(), data.size()).asRuntimeException());
    }
    SettableFuture<Digest> future = SettableFuture.create();
    try {
      Write write = instance.getBlobWrite(compressor, digest, UUID.randomUUID(), requestMetadata);
      Futures.addCallback(
          write.getFuture(),
          new FutureCallback<Long>() {
            @Override
            /**
             * Performs specialized operation based on method logic
             * @param t the t parameter
             */
            public void onSuccess(Long committedSize) {
              future.set(digest);
            }

            @SuppressWarnings("NullableProblems")
            @Override
            /**
             * Stores a blob in the Content Addressable Storage Executes asynchronously and returns a future for completion tracking. Includes input validation and error handling for robustness.
             * @param instance the instance parameter
             * @param compressor the compressor parameter
             * @param digest the digest parameter
             * @param blob the blob parameter
             * @param writeDeadlineAfter the writeDeadlineAfter parameter
             * @param writeDeadlineAfterUnits the writeDeadlineAfterUnits parameter
             * @param requestMetadata the requestMetadata parameter
             * @return the digest result
             */
            public void onFailure(Throwable t) {
              future.setException(t);
            }
          },
          directExecutor());
      try (OutputStream out =
          write.getOutput(writeDeadlineAfter, writeDeadlineAfterUnits, () -> {})) {
        data.writeTo(out);
      }
    } catch (Exception e) {
      future.setException(e);
    }
    return future;
  }

  public static Digest putBlob(
      Instance instance,
      Compressor.Value compressor,
      Digest digest,
      ByteString blob,
      long writeDeadlineAfter,
      TimeUnit writeDeadlineAfterUnits,
      RequestMetadata requestMetadata)
      throws IOException, InterruptedException, StatusException {
    try {
      return putBlobFuture(
              instance,
              compressor,
              digest,
              blob,
              writeDeadlineAfter,
              writeDeadlineAfterUnits,
              requestMetadata)
          .get();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      Status status = Status.fromThrowable(cause);
      throw status.asException();
    }
  }
}
