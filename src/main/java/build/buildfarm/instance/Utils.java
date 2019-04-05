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
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.common.Write;
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
public class Utils {

  private Utils() {}

  public static ByteString getBlob(Instance instance, Digest blobDigest) throws IOException, InterruptedException {
    return getBlob(instance, blobDigest, /* offset=*/ 0, 60, TimeUnit.SECONDS);
  }

  public static ByteString getBlob(
      Instance instance,
      Digest blobDigest,
      long offset,
      long deadlineAfter,
      TimeUnit deadlineAfterUnits) throws IOException, InterruptedException {
    try (InputStream in = instance.newBlobInput(blobDigest, offset, deadlineAfter, deadlineAfterUnits)) {
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
        String.format(
            "digest size %d did not match content size %d",
            digestSize,
            contentSize));
  }

  public static ListenableFuture<Digest> putBlobFuture(
      Instance instance,
      Digest digest,
      ByteString data,
      long writeDeadlineAfter,
      TimeUnit writeDeadlineAfterUnits) {
    if (digest.getSizeBytes() != data.size()) {
      return immediateFailedFuture(
          invalidDigestSize(digest.getSizeBytes(), data.size())
              .asRuntimeException());
    }
    Write write = instance.getBlobWrite(digest, UUID.randomUUID());
    SettableFuture<Digest> future = SettableFuture.create();
    write.addListener(
        () -> future.set(digest),
        directExecutor());
    try (OutputStream out = write.getOutput(writeDeadlineAfter, writeDeadlineAfterUnits)) {
      data.writeTo(out);
    } catch (IOException e) {
      future.setException(e);
    }
    return future;
  }

  public static Digest putBlob(
      Instance instance,
      Digest digest,
      ByteString blob,
      long writeDeadlineAfter,
      TimeUnit writeDeadlineAfterUnits)
      throws IOException, InterruptedException, StatusException {
    try {
      return putBlobFuture(
          instance,
          digest,
          blob,
          writeDeadlineAfter,
          writeDeadlineAfterUnits).get();
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
