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

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.instance.Instance.ChunkObserver;
import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutionException;

/** Utility methods for the instance package. * */
public class Utils {

  private Utils() {}

  public static ByteString getBlob(Instance instance, Digest blobDigest) throws IOException, InterruptedException {
    try (InputStream in = instance.newStreamInput(instance.getBlobName(blobDigest), 0)) {
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

  public static ListenableFuture<Digest> putBlobFuture(Instance instance, Digest digest, ByteString data) {
    if (digest.getSizeBytes() != data.size()) {
      return immediateFailedFuture(
          invalidDigestSize(digest.getSizeBytes(), data.size())
              .asRuntimeException());
    }
    ChunkObserver observer = instance.getWriteBlobObserver(digest);
    observer.onNext(data);
    observer.onCompleted();
    return transform(
        observer.getCommittedFuture(),
        new Function<Long, Digest>() {
          @Override
          public Digest apply(Long committedSize) {
            if (committedSize != digest.getSizeBytes()) {
              throw invalidDigestSize(committedSize, digest.getSizeBytes())
                  .asRuntimeException();
            }
            return digest;
          }
        });
  }

  public static Digest putBlob(Instance instance, Digest digest, ByteString blob)
      throws IOException, InterruptedException, StatusException {
    try {
      return putBlobFuture(instance, digest, blob).get();
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
