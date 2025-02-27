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

package build.buildfarm.common;

import static com.google.common.truth.Truth.assertThat;

import build.bazel.remote.execution.v2.Compressor;
import build.buildfarm.v1test.Digest;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.InputStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class EmptyInputStreamFactoryTest {
  private static final DigestUtil DIGEST_UTIL = new DigestUtil(DigestUtil.HashFunction.SHA256);

  @Test
  public void emptyDigestIsNotDelegated() throws IOException, InterruptedException {
    EmptyInputStreamFactory emptyFactory =
        new EmptyInputStreamFactory(
            (compressor, digest, offset) -> {
              throw new IOException("invalid");
            });
    InputStream in =
        emptyFactory.newInput(
            Compressor.Value.IDENTITY, Digest.getDefaultInstance(), /* offset= */ 0);
    assertThat(in.read()).isEqualTo(-1);
  }

  @Test
  public void nonEmptyDigestIsDelegated() throws IOException, InterruptedException {
    ByteString content = ByteString.copyFromUtf8("Hello, World");
    Digest contentDigest = DIGEST_UTIL.compute(content);
    EmptyInputStreamFactory emptyFactory =
        new EmptyInputStreamFactory(
            (compressor, digest, offset) -> {
              if (digest.equals(contentDigest)) {
                return content.newInput();
              }
              throw new IOException("invalid");
            });
    InputStream in =
        emptyFactory.newInput(Compressor.Value.IDENTITY, contentDigest, /* offset= */ 0);
    assertThat(ByteString.readFrom(in)).isEqualTo(content);
  }
}
