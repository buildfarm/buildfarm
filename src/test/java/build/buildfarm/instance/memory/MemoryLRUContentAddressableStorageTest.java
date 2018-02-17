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

package build.buildfarm.instance.memory;

import static com.google.common.truth.Truth.assertThat;

import build.buildfarm.instance.memory.MemoryLRUContentAddressableStorage;
import build.buildfarm.common.ContentAddressableStorage;
import build.buildfarm.common.ContentAddressableStorage.Blob;
import build.buildfarm.common.DigestUtil;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.protobuf.ByteString;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MemoryLRUContentAddressableStorageTest {
  private class CountingRunnable implements Runnable {
    int runCount = 0;

    @Override
    public void run() {
      runCount++;
    }
  };

  @Test
  public void expireShouldCallOnExpiration() {
    ContentAddressableStorage storage = new MemoryLRUContentAddressableStorage(10);

    DigestUtil sha1DigestUtil = new DigestUtil(DigestUtil.HashFunction.SHA256);
    CountingRunnable onExpiration = new CountingRunnable();
    storage.put(new Blob(ByteString.copyFromUtf8("stdout"), sha1DigestUtil), onExpiration);
    assertThat(onExpiration.runCount).isEqualTo(0);
    storage.put(new Blob(ByteString.copyFromUtf8("stderr"), sha1DigestUtil));
    assertThat(onExpiration.runCount).isEqualTo(1);
  }

  @Test
  public void duplicateEntryRegistersMultipleOnExpiration() {
    ContentAddressableStorage storage = new MemoryLRUContentAddressableStorage(10);

    DigestUtil sha1DigestUtil = new DigestUtil(DigestUtil.HashFunction.SHA256);
    CountingRunnable onExpiration = new CountingRunnable();
    storage.put(new Blob(ByteString.copyFromUtf8("stdout"), sha1DigestUtil), onExpiration);
    storage.put(new Blob(ByteString.copyFromUtf8("stdout"), sha1DigestUtil), onExpiration);
    assertThat(onExpiration.runCount).isEqualTo(0);
    storage.put(new Blob(ByteString.copyFromUtf8("stderr"), sha1DigestUtil));
    assertThat(onExpiration.runCount).isEqualTo(2);
  }
}
