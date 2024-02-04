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

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.Write;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MemoryWriteOutputStreamTest {
  private static final DigestUtil DIGEST_UTIL = new DigestUtil(HashFunction.SHA256);

  @Test
  public void asyncWriteCompletionIsComplete() throws IOException {
    ContentAddressableStorage cas = mock(ContentAddressableStorage.class);
    ByteString content = ByteString.copyFromUtf8("Hello, World!");
    Digest digest = DIGEST_UTIL.compute(content);
    SettableFuture<ByteString> writtenFuture = SettableFuture.create();
    Write write = new MemoryWriteOutputStream(cas, digest, writtenFuture);
    content.substring(0, 6).writeTo(write.getOutput(1, TimeUnit.SECONDS, () -> {}));
    writtenFuture.set(content);
    assertThat(write.isComplete()).isTrue();
    assertThat(write.getCommittedSize()).isEqualTo(digest.getSizeBytes());
    verifyNoInteractions(cas);
  }
}
