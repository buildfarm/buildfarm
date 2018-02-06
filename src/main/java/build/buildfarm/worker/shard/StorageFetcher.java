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

import build.buildfarm.common.ContentAddressableStorage;
import build.buildfarm.worker.Fetcher;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.protobuf.ByteString;
import java.io.IOException;

class StorageFetcher implements Fetcher {
  private final ContentAddressableStorage storage;
  private final Fetcher delegate;

  StorageFetcher(ContentAddressableStorage storage, Fetcher delegate) {
    this.storage = storage;
    this.delegate = delegate;
  }

  @Override
  public ByteString fetchBlob(Digest blobDigest) throws InterruptedException, IOException {
    synchronized (storage.acquire(blobDigest)) {
      // necessary, since storage.get would loop back to this function
      try {
        if (storage.contains(blobDigest)) {
          return storage.get(blobDigest).getData();
        }
      } finally {
        storage.release(blobDigest);
      }
    }

    return delegate.fetchBlob(blobDigest);
  }
};
