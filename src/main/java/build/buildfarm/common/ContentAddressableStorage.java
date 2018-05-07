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

package build.buildfarm.common;

import build.buildfarm.common.ThreadSafety.ThreadSafe;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.protobuf.ByteString;

public interface ContentAddressableStorage {
  /**
   * Blob storage for the CAS. This class should be used at all times when interacting with
   * complete blobs in order to cut down on independent digest computation.
   */
  public static final class Blob {
    private final Digest digest;
    private final ByteString data;

    public Blob(ByteString data, DigestUtil digestUtil) {
      digest = digestUtil.compute(data);
      this.data = data;
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

  /** Indicates presence in the CAS for a digest. */
  @ThreadSafe
  boolean contains(Digest digest);

  /** Retrieve a value from the CAS. */
  @ThreadSafe
  Blob get(Digest digest);

  /** Insert a blob into the CAS. */
  @ThreadSafe
  void put(Blob blob);

  /**
   * Insert a value into the CAS with expiration callback.
   *
   * <p>The callback provided will be run after the value is expired
   * and removed from the storage. Successive calls to this method
   * for a unique blob digest will register additional callbacks, does
   * not deduplicate by callback, and the order of which is not
   * guaranteed for invocation.
   */
  @ThreadSafe
  void put(Blob blob, Runnable onExpiration);

  @ThreadSafe
  Object acquire(Digest digest);

  @ThreadSafe
  void release(Digest digest);
}
