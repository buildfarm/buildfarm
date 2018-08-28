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

package build.buildfarm.cas;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import build.buildfarm.cas.ContentAddressableStorage.Blob;
import build.buildfarm.common.DigestUtil;
import build.bazel.remote.execution.v2.Digest;
import com.google.protobuf.ByteString;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MemoryCASTest {
  @Test
  public void expireShouldCallOnExpiration() throws InterruptedException {
    ContentAddressableStorage storage = new MemoryCAS(10);

    DigestUtil sha1DigestUtil = new DigestUtil(DigestUtil.HashFunction.SHA256);
    Runnable mockOnExpiration = mock(Runnable.class);
    storage.put(new Blob(ByteString.copyFromUtf8("stdout"), sha1DigestUtil), mockOnExpiration);
    verify(mockOnExpiration, never()).run();
    storage.put(new Blob(ByteString.copyFromUtf8("stderr"), sha1DigestUtil));
    verify(mockOnExpiration, times(1)).run();
  }

  @Test
  public void expireShouldOccurAtLimitExactly() throws InterruptedException {
    ContentAddressableStorage storage = new MemoryCAS(11);

    DigestUtil sha1DigestUtil = new DigestUtil(DigestUtil.HashFunction.SHA256);
    Runnable mockOnExpiration = mock(Runnable.class);
    storage.put(new Blob(ByteString.copyFromUtf8("stdin"), sha1DigestUtil), mockOnExpiration);
    storage.put(new Blob(ByteString.copyFromUtf8("stdout"), sha1DigestUtil), mockOnExpiration);
    verify(mockOnExpiration, never()).run();
    storage.put(new Blob(ByteString.copyFromUtf8("a"), sha1DigestUtil));
    verify(mockOnExpiration, times(1)).run();
  }

  @Test
  public void duplicateEntryRegistersMultipleOnExpiration() throws InterruptedException {
    ContentAddressableStorage storage = new MemoryCAS(10);

    DigestUtil sha1DigestUtil = new DigestUtil(DigestUtil.HashFunction.SHA256);
    Runnable mockOnExpiration = mock(Runnable.class);
    storage.put(new Blob(ByteString.copyFromUtf8("stdout"), sha1DigestUtil), mockOnExpiration);
    storage.put(new Blob(ByteString.copyFromUtf8("stdout"), sha1DigestUtil), mockOnExpiration);
    verify(mockOnExpiration, never()).run();
    storage.put(new Blob(ByteString.copyFromUtf8("stderr"), sha1DigestUtil));
    verify(mockOnExpiration, times(2)).run();
  }
}
