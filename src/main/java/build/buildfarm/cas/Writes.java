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

import static com.google.common.io.ByteStreams.nullOutputStream;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.cas.ContentAddressableStorage.Blob;
import build.buildfarm.common.Write;
import build.buildfarm.v1test.BlobWriteKey;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.protobuf.ByteString;
import java.io.OutputStream;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

class Writes {
  private final ContentAddressableStorage storage;
  private final Cache<BlobWriteKey, Write> blobWrites = CacheBuilder.newBuilder()
      .expireAfterWrite(10, TimeUnit.MINUTES)
      .build();
  private final Cache<Digest, SettableFuture<ByteString>> writesInProgress = CacheBuilder.newBuilder()
      .expireAfterWrite(10, TimeUnit.MINUTES)
      .build();

  static class CompleteWrite implements Write {
    private final long committedSize;

    CompleteWrite(long committedSize) {
      this.committedSize = committedSize;
    }

    @Override
    public long getCommittedSize() {
      return committedSize;
    }

    @Override
    public boolean isComplete() {
      return true;
    }

    @Override
    public OutputStream getOutput() {
      return nullOutputStream();
    }

    @Override
    public void reset() {
    }

    @Override
    public void addListener(Runnable onCompleted, Executor executor) {
      executor.execute(onCompleted);
    }
  }

  Writes(ContentAddressableStorage storage) {
    this.storage = storage;
  }

  Write get(Digest digest, UUID uuid) {
    if (digest.getSizeBytes() == 0) {
      return new CompleteWrite(0);
    }
    return getNonEmpty(digest, uuid);
  }

  private synchronized Write getNonEmpty(Digest digest, UUID uuid) {
    Blob blob = storage.get(digest);
    if (blob != null) {
      return new CompleteWrite(digest.getSizeBytes());
    }
    return get(BlobWriteKey.newBuilder()
        .setDigest(digest)
        .setIdentifier(uuid.toString())
        .build());
  }

  private Write get(BlobWriteKey key) {
    try {
      return blobWrites.get(key, () -> newWrite(key.getDigest()));
    } catch (ExecutionException e) {
      throw new UncheckedExecutionException(e);
    }
  }

  private Write newWrite(Digest digest) {
    return new MemoryWriteOutputStream(
        storage,
        digest,
        getFuture(digest));
  }

  SettableFuture<ByteString> getFuture(Digest digest) {
    try {
      return writesInProgress.get(
          digest,
          () -> {
            SettableFuture<ByteString> blobWritten = SettableFuture.create();
            blobWritten.addListener(
                () -> writesInProgress.invalidate(digest),
                directExecutor());
            return blobWritten;
          });
    } catch (ExecutionException e) {
      throw new UncheckedExecutionException(e.getCause());
    }
  }
}
