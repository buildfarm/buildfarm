// Copyright 2018 The Bazel Authors. All rights reserved.
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
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.Write;
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
  public static ListenableFuture<Digest> putBlobFuture(
      Instance instance,
      Compressor.Value compressor,
      Digest digest,
      DigestFunction.Value digestFunction,
      ByteString data,
      long writeDeadlineAfter,
      TimeUnit writeDeadlineAfterUnits,
      RequestMetadata requestMetadata) {
    if (digest.getSizeBytes() != data.size()) {
      return immediateFailedFuture(
          invalidDigestSize(digest.getSizeBytes(), data.size()).asRuntimeException());
    }
    SettableFuture<Digest> future = SettableFuture.create();
    try {
      Write write =
          instance.getBlobWrite(
              compressor, digest, digestFunction, UUID.randomUUID(), requestMetadata);
      Futures.addCallback(
          write.getFuture(),
          new FutureCallback<Long>() {
            @Override
            public void onSuccess(Long committedSize) {
              future.set(digest);
            }

            @SuppressWarnings("NullableProblems")
            @Override
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
      DigestFunction.Value digestFunction,
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
              digestFunction,
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
