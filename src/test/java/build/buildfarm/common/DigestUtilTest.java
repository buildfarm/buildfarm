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

package build.buildfarm.common;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.DigestFunction;
import build.buildfarm.common.DigestUtil.HashFunction;
import com.google.protobuf.ByteString;
import java.io.IOException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DigestUtilTest {
  @Test
  public void buildThrowsOnInvalidHashCode() {
    DigestUtil digestUtil = new DigestUtil(HashFunction.MD5);
    NumberFormatException expected =
        assertThrows(
            NumberFormatException.class,
            () -> {
              digestUtil.build("foo", 3);
            });
    assertThat(expected.getMessage()).isEqualTo("[foo] is not a valid MD5 hash.");
  }

  @Test
  public void builtDigestMatchesFields() {
    DigestUtil digestUtil = new DigestUtil(HashFunction.MD5);
    String bazelMd5Hash = "24ef4c36ec66c15ef9f0c96fe27c0e0b";
    long payloadSizeInBytes = 5;
    Digest digest = digestUtil.build(bazelMd5Hash, payloadSizeInBytes);
    assertThat(digest.getHash()).isEqualTo(bazelMd5Hash);
    assertThat(digest.getSizeBytes()).isEqualTo(payloadSizeInBytes);
  }

  @Test
  public void computesMd5Hash() {
    ByteString content = ByteString.copyFromUtf8("bazel");
    DigestUtil digestUtil = new DigestUtil(HashFunction.MD5);
    Digest digest = digestUtil.compute(content);
    assertThat(digest.getHash()).isEqualTo("24ef4c36ec66c15ef9f0c96fe27c0e0b");
  }

  @Test
  public void computesSha1Hash() {
    ByteString content = ByteString.copyFromUtf8("bazel");
    DigestUtil digestUtil = new DigestUtil(HashFunction.SHA1);
    Digest digest = digestUtil.compute(content);
    assertThat(digest.getHash()).isEqualTo("287d5d65c10a8609e9c504c81f650b0e1669a824");
  }

  @Test
  public void computesSha256Hash() {
    ByteString content = ByteString.copyFromUtf8("bazel");
    DigestUtil digestUtil = new DigestUtil(HashFunction.SHA256);
    Digest digest = digestUtil.compute(content);
    assertThat(digest.getHash())
        .isEqualTo("aa0e09c406dd0db1a3bb250216045e81644d26c961c0e8c34e8a0354476ca6d4");
  }

  @Test(expected = IllegalArgumentException.class)
  public void unrecognizedHashFunctionThrows() {
    HashFunction.get(DigestFunction.Value.UNRECOGNIZED);
  }

  @Test
  public void hashFunctionsMatchHashFunctions() {
    assertThat(HashFunction.get(DigestFunction.Value.MD5)).isEqualTo(HashFunction.MD5);
    assertThat(HashFunction.get(DigestFunction.Value.SHA1)).isEqualTo(HashFunction.SHA1);
    assertThat(HashFunction.get(DigestFunction.Value.SHA256)).isEqualTo(HashFunction.SHA256);
  }

  @Test
  public void forHashMatchesName() {
    assertThat(DigestUtil.forHash("MD5").empty())
        .isEqualTo(new DigestUtil(HashFunction.get(DigestFunction.Value.MD5)).empty());
    assertThat(DigestUtil.forHash("SHA1").empty())
        .isEqualTo(new DigestUtil(HashFunction.get(DigestFunction.Value.SHA1)).empty());
    assertThat(DigestUtil.forHash("SHA256").empty())
        .isEqualTo(new DigestUtil(HashFunction.get(DigestFunction.Value.SHA256)).empty());
  }

  @Test
  public void parseDigestMatches() throws IOException {
    DigestUtil digestUtil = DigestUtil.forHash("MD5");
    Digest digest = digestUtil.compute(ByteString.copyFromUtf8("stdout"));
    assertThat(DigestUtil.parseDigest(DigestUtil.toString(digest))).isEqualTo(digest);
  }
}
