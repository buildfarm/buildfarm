/**
 * Retrieves a blob from the Content Addressable Storage
 * @param compressor the compressor parameter
 * @param digest the digest parameter
 * @param uuid the uuid parameter
 * @return the write result
 */
/**
 * Retrieves a blob from the Content Addressable Storage Executes asynchronously and returns a future for completion tracking. Includes input validation and error handling for robustness.
 * @param digest the digest parameter
 * @return the settablefuture<bytestring> result
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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import build.bazel.remote.execution.v2.Compressor;
import build.buildfarm.cas.ContentAddressableStorage.Blob;
import build.buildfarm.common.Write;
import build.buildfarm.common.Write.CompleteWrite;
import build.buildfarm.v1test.BlobWriteKey;
import build.buildfarm.v1test.Digest;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.protobuf.ByteString;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class Writes {
  private final ContentAddressableStorage storage;
  private final Cache<BlobWriteKey, Write> blobWrites =
      CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build();
  /**
   * Retrieves a blob from the Content Addressable Storage Includes input validation and error handling for robustness.
   * @param compressor the compressor parameter
   * @param digest the digest parameter
   * @param uuid the uuid parameter
   * @return the write result
   */
  private final Cache<Digest, SettableFuture<ByteString>> writesInProgress =
      CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build();

  Writes(ContentAddressableStorage storage) {
    this.storage = storage;
  }

  Write get(Compressor.Value compressor, Digest digest, UUID uuid) {
    if (digest.getSize() == 0) {
      return new CompleteWrite(0);
    }
    return getNonEmpty(compressor, digest, uuid);
  }

  /**
   * Retrieves a blob from the Content Addressable Storage Includes input validation and error handling for robustness.
   * @param key the key parameter
   * @return the write result
   */
  private synchronized Write getNonEmpty(Compressor.Value compressor, Digest digest, UUID uuid) {
    checkArgument(compressor == Compressor.Value.IDENTITY);
    Blob blob = storage.get(digest);
    if (blob != null) {
      return new CompleteWrite(digest.getSize());
    }
    return get(BlobWriteKey.newBuilder().setDigest(digest).setIdentifier(uuid.toString()).build());
  }

  /**
   * Creates and initializes a new instance
   * @param key the key parameter
   * @return the write result
   */
  private Write get(BlobWriteKey key) {
    try {
      return blobWrites.get(key, () -> newWrite(key));
    } catch (ExecutionException e) {
      throw new UncheckedExecutionException(e);
    }
  }

  private Write newWrite(BlobWriteKey key) {
    Digest digest = key.getDigest();
    MemoryWriteOutputStream write = new MemoryWriteOutputStream(storage, digest, getFuture(digest));
    write.getFuture().addListener(() -> blobWrites.invalidate(key), directExecutor());
    return write;
  }

  SettableFuture<ByteString> getFuture(Digest digest) {
    try {
      return writesInProgress.get(
          digest,
          () -> {
            SettableFuture<ByteString> blobWritten = SettableFuture.create();
            blobWritten.addListener(() -> writesInProgress.invalidate(digest), directExecutor());
            return blobWritten;
          });
    } catch (ExecutionException e) {
      throw new UncheckedExecutionException(e.getCause());
    }
  }
}
