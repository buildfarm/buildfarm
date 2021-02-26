// Copyright 2017 The Bazel Authors. All rights reserved.
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

import build.bazel.remote.execution.v2.BatchReadBlobsResponse.Response;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.EntryLimitException;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.common.ThreadSafety.ThreadSafe;
import build.buildfarm.common.Write;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import io.grpc.stub.ServerCallStreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

public interface ContentAddressableStorage extends InputStreamFactory {
  long UNLIMITED_ENTRY_SIZE_MAX = -1;

  /**
   * Blob storage for the CAS. This class should be used at all times when interacting with complete
   * blobs in order to cut down on independent digest computation.
   */
  final class Blob {
    private final Digest digest;
    private final ByteString data;

    public Blob(ByteString data, DigestUtil digestUtil) {
      this.data = data;
      digest = digestUtil.compute(data);
    }

    public Blob(ByteString data, Digest digest) {
      this.data = data;
      this.digest = digest;
    }

    public Digest getDigest() {
      return digest;
    }

    public ByteString getData() {
      return data;
    }

    public long size() {
      return digest.getSizeBytes();
    }

    public boolean isEmpty() {
      return size() == 0;
    }
  }

  /**
   * Indicates presence in the CAS for a single digest.
   *
   * <p>If supported, a size_bytes of -1 may be used to look up the size of a digest A size
   * mismatch, if partial key selection is supported, may result in correction
   */
  @ThreadSafe
  boolean contains(Digest digest, Digest.Builder result);

  /** Indicates presence in the CAS for a sequence of digests. */
  @ThreadSafe
  Iterable<Digest> findMissingBlobs(Iterable<Digest> digests) throws InterruptedException;

  /** Retrieve a value from the CAS. */
  @ThreadSafe
  Blob get(Digest digest);

  /** Retrieve a set of blobs from the CAS represented by a future. */
  ListenableFuture<Iterable<Response>> getAllFuture(Iterable<Digest> digests);

  @ThreadSafe
  InputStream newInput(Digest digest, long offset) throws IOException;

  /** Retrieve a value from the CAS by streaming content when ready */
  @ThreadSafe
  void get(
      Digest digest,
      long offset,
      long count,
      ServerCallStreamObserver<ByteString> blobObserver,
      RequestMetadata requestMetadata);

  @ThreadSafe
  Write getWrite(Digest digest, UUID uuid, RequestMetadata requestMetadata)
      throws EntryLimitException;

  /** Insert a blob into the CAS. */
  @ThreadSafe
  void put(Blob blob) throws EntryLimitException, InterruptedException;

  /**
   * Insert a value into the CAS with expiration callback.
   *
   * <p>The callback provided will be run after the value is expired and removed from the storage.
   * Successive calls to this method for a unique blob digest will register additional callbacks,
   * does not deduplicate by callback, and the order of which is not guaranteed for invocation.
   */
  @ThreadSafe
  void put(Blob blob, Runnable onExpiration) throws InterruptedException;

  @ThreadSafe
  long maxEntrySize();
}
