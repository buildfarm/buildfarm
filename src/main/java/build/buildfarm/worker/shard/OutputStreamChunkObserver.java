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

package build.buildfarm.worker.shard;

import build.buildfarm.instance.Instance.ChunkObserver;
import build.buildfarm.worker.CASFileCache.DigestMismatchException;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import java.io.IOException;
import java.io.OutputStream;

abstract class OutputStreamChunkObserver implements ChunkObserver {
  private final SettableFuture<Long> committedFuture = SettableFuture.create();
  // use lazy?
  private OutputStream stream = null;
  private long committedSize = 0;
  private boolean alreadyExists = false;

  @Override
  public long getCommittedSize() {
    return committedSize;
  }

  @Override
  public ListenableFuture<Long> getCommittedFuture() {
    return committedFuture;
  }

  @Override
  public void reset() {
    if (stream != null) {
      try {
        stream.close();
      } catch (IOException e) {
        // ignore exception on reset
      }
    }
    alreadyExists = false;
    stream = null;

    committedSize = 0;
  }

  protected abstract OutputStream newOutput() throws IOException;

  @Override
  public void onNext(ByteString chunk) {
    try {
      if (!alreadyExists && stream == null) {
        stream = newOutput();
        alreadyExists = stream == null;
      }
      if (!alreadyExists) {
        chunk.writeTo(stream);
      }
    } catch (IOException e) {
      if (e.getCause() instanceof InterruptedException) {
        Thread.currentThread().interrupt();
        return;
      }
      throw Status.INTERNAL.withCause(e).asRuntimeException();
    }
    committedSize += chunk.size();
  }

  @Override
  public void onCompleted() {
    if (stream != null) {
      try {
        stream.close();
      } catch (DigestMismatchException e) {
        throw Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException();
      } catch (IOException e) {
        throw Status.INTERNAL.withCause(e).asRuntimeException();
      }
    }
    committedFuture.set(committedSize);
  }

  @Override
  public void onError(Throwable t) {
    if (stream != null) {
      try {
        stream.close(); // relies on validity check in CAS
      } catch (IOException e) {
        // ignore?
      }
    }
    committedFuture.setException(t);
  }
};
