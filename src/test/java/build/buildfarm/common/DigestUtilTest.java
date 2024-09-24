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

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.DigestFunction;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.v1test.Digest;
import com.google.common.collect.Iterables;
import com.google.common.hash.HashingOutputStream;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DigestUtilTest {
  private static final ByteString bazelContent = ByteString.copyFromUtf8("bazel");
  private static final String bazelMd5Hash = "24ef4c36ec66c15ef9f0c96fe27c0e0b";
  private static final String bazelSha1Hash = "287d5d65c10a8609e9c504c81f650b0e1669a824";
  private static final String bazelSha256Hash =
      "aa0e09c406dd0db1a3bb250216045e81644d26c961c0e8c34e8a0354476ca6d4";
  private static final String bazelSha384Hash =
      "355937f5f95da9265b27ebf97992bb4db13130bad5796a11148f93c13ada3efb64ca0e4c3a7fec23bb130f26f789972d";
  private static final String bazelSha512Hash =
      "c0928504979921cab0fbca6211131a3f40a4a597b6299f902856e365b51c6b3278735123f53390f84576d4d57bf6b088a99b8d92720581c23d14754b089bb150";
  private static final String bazelBlake3Hash =
      "8d6a50ef58e4214c5f1bec4e570c43661b1c356c149a84491e3defa39bced9e5";

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
    Digest digest = digestUtil.build(bazelMd5Hash, bazelContent.size());
    assertThat(digest.getHash()).isEqualTo(bazelMd5Hash);
    assertThat(digest.getSize()).isEqualTo(bazelContent.size());
  }

  @Test
  public void computesMd5Hash() {
    DigestUtil digestUtil = new DigestUtil(HashFunction.MD5);
    Digest digest = digestUtil.compute(bazelContent);
    assertThat(digest.getHash()).isEqualTo(bazelMd5Hash);
  }

  @Test
  public void computesSha1Hash() {
    DigestUtil digestUtil = new DigestUtil(HashFunction.SHA1);
    Digest digest = digestUtil.compute(bazelContent);
    assertThat(digest.getHash()).isEqualTo(bazelSha1Hash);
  }

  @Test
  public void computesSha256Hash() {
    DigestUtil digestUtil = new DigestUtil(HashFunction.SHA256);
    Digest digest = digestUtil.compute(bazelContent);
    assertThat(digest.getHash()).isEqualTo(bazelSha256Hash);
  }

  @Test
  public void computesSha384Hash() {
    DigestUtil digestUtil = new DigestUtil(HashFunction.SHA384);
    Digest digest = digestUtil.compute(bazelContent);
    assertThat(digest.getHash()).isEqualTo(bazelSha384Hash);
  }

  @Test
  public void computesSha512Hash() {
    DigestUtil digestUtil = new DigestUtil(HashFunction.SHA512);
    Digest digest = digestUtil.compute(bazelContent);
    assertThat(digest.getHash()).isEqualTo(bazelSha512Hash);
  }

  @Test
  public void computeEmptyIsDefault() {
    DigestUtil digestUtil = new DigestUtil(HashFunction.BLAKE3);
    Digest digest = digestUtil.compute(ByteString.empty());
    assertThat(digest == Digest.getDefaultInstance()).isTrue();
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
    assertThat(HashFunction.get(DigestFunction.Value.SHA384)).isEqualTo(HashFunction.SHA384);
    assertThat(HashFunction.get(DigestFunction.Value.SHA512)).isEqualTo(HashFunction.SHA512);
  }

  @Test
  public void forHashMatchesName() {
    assertThat(DigestUtil.forHash("NOPE")).isNull();

    assertThat(DigestUtil.forHash("MD5").getDigestFunction()).isEqualTo(DigestFunction.Value.MD5);
    assertThat(DigestUtil.forHash("SHA1").getDigestFunction()).isEqualTo(DigestFunction.Value.SHA1);
    assertThat(DigestUtil.forHash("SHA256").getDigestFunction())
        .isEqualTo(DigestFunction.Value.SHA256);
    assertThat(DigestUtil.forHash("SHA384").getDigestFunction())
        .isEqualTo(DigestFunction.Value.SHA384);
    assertThat(DigestUtil.forHash("SHA512").getDigestFunction())
        .isEqualTo(DigestFunction.Value.SHA512);
  }

  @Test
  public void toStringMatches() {
    DigestUtil digestUtil = DigestUtil.forHash("BLAKE3");
    Digest blake3Digest = digestUtil.compute(bazelContent);
    assertThat(DigestUtil.toString(blake3Digest))
        .isEqualTo("blake3/" + bazelBlake3Hash + "/" + bazelContent.size());
  }

  @Test
  public void parseDigestMatches() {
    DigestUtil digestUtil = DigestUtil.forHash("MD5");
    Digest digest = digestUtil.compute(bazelContent);
    assertThat(DigestUtil.parseDigest(DigestUtil.toString(digest))).isEqualTo(digest);

    digestUtil = DigestUtil.forHash("BLAKE3");
    Digest blake3Digest = digestUtil.compute(bazelContent);
    assertThat(DigestUtil.parseDigest("blake3/" + bazelBlake3Hash + "/" + bazelContent.size()))
        .isEqualTo(blake3Digest);
  }

  @Test
  public void inferDigestFunctionInterpretsUnknown() {
    Digest bazelBlake3Digest = DigestUtil.forHash("BLAKE3").compute(bazelContent);
    assertThat(
            DigestUtil.inferDigestFunction(
                DigestFunction.Value.BLAKE3, bazelBlake3Digest.getHash()))
        .isEqualTo(DigestFunction.Value.BLAKE3);

    assertThat(DigestUtil.inferDigestFunction(bazelMd5Hash)).isEqualTo(DigestFunction.Value.MD5);
    assertThat(DigestUtil.inferDigestFunction(bazelSha1Hash)).isEqualTo(DigestFunction.Value.SHA1);
    assertThat(DigestUtil.inferDigestFunction(bazelSha256Hash))
        .isEqualTo(DigestFunction.Value.SHA256);
    assertThat(DigestUtil.inferDigestFunction(bazelSha384Hash))
        .isEqualTo(DigestFunction.Value.SHA384);
    assertThat(DigestUtil.inferDigestFunction(bazelSha512Hash))
        .isEqualTo(DigestFunction.Value.SHA512);
  }

  @Test
  public void actionKeyIsSuitableMapKey() {
    DigestUtil digestUtil = DigestUtil.forHash("MD5");
    Digest bazelDigest = digestUtil.compute(bazelContent);
    Digest keyDigest = digestUtil.compute(bazelContent);
    // assert different refs
    assertThat(keyDigest != bazelDigest).isTrue();
    ActionKey bazelKey = DigestUtil.asActionKey(bazelDigest);
    ActionKey otherKey = DigestUtil.asActionKey(keyDigest);
    assertThat(bazelKey != otherKey).isTrue();
    assertThat(bazelKey.hashCode()).isEqualTo(otherKey.hashCode());
    assertThat(bazelKey).isEqualTo(otherKey);

    // object type mismatch
    assertThat(bazelKey).isNotEqualTo(bazelDigest);
  }

  @Test
  public void parseDigestFunctionMatches() {
    assertThat(DigestUtil.parseDigestFunction("notahashsize"))
        .isEqualTo(DigestFunction.Value.UNKNOWN);

    assertThat(DigestUtil.parseDigestFunction(bazelMd5Hash)).isEqualTo(DigestFunction.Value.MD5);
    assertThat(DigestUtil.parseDigestFunction(bazelSha1Hash)).isEqualTo(DigestFunction.Value.SHA1);
    assertThat(DigestUtil.parseDigestFunction(bazelSha256Hash))
        .isEqualTo(DigestFunction.Value.SHA256);
    assertThat(DigestUtil.parseDigestFunction(bazelSha384Hash))
        .isEqualTo(DigestFunction.Value.SHA384);
    assertThat(DigestUtil.parseDigestFunction(bazelSha512Hash))
        .isEqualTo(DigestFunction.Value.SHA512);
  }

  @Test
  public void parseHashOfUnknownIsNull() {
    assertThat(DigestUtil.parseHash("notahashsize")).isNull();
  }

  @Test
  public void toDigestMatchesFromDigest() {
    DigestUtil digestUtil = DigestUtil.forHash("MD5");
    Digest digest = digestUtil.compute(bazelContent);
    build.bazel.remote.execution.v2.Digest reapiDigest = DigestUtil.toDigest(digest);
    assertThat(digestUtil.fromDigest(reapiDigest, DigestFunction.Value.MD5)).isEqualTo(digest);
    assertThat(digestUtil.toDigest(reapiDigest)).isEqualTo(digest);
  }

  private Action createAction(DigestUtil digestUtil) {
    build.bazel.remote.execution.v2.Digest bazelDigest =
        DigestUtil.toDigest(digestUtil.compute(bazelContent));
    return Action.newBuilder()
        .setCommandDigest(bazelDigest)
        .setInputRootDigest(bazelDigest)
        .build();
  }

  @Test
  public void computeMessageIsDigest() {
    DigestUtil digestUtil = DigestUtil.forHash("MD5");
    Action action = createAction(digestUtil);
    Digest actionDigest = digestUtil.compute(action);
    assertThat(actionDigest).isEqualTo(digestUtil.compute(action.toByteString()));
  }

  @Test
  public void computeActionKeyEqualToDigest() {
    DigestUtil digestUtil = DigestUtil.forHash("MD5");
    Action action = createAction(digestUtil);
    assertThat(digestUtil.computeActionKey(action))
        .isEqualTo(DigestUtil.asActionKey(digestUtil.compute(action.toByteString())));
  }

  @Test
  public void forDigestInfersDigestFunction() {
    build.bazel.remote.execution.v2.Digest digest =
        build.bazel.remote.execution.v2.Digest.newBuilder()
            .setSizeBytes(bazelContent.size())
            .build();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DigestUtil.forDigest(digest.toBuilder().setHash("notahash").build())
                .getDigestFunction());
    assertThat(
            DigestUtil.forDigest(digest.toBuilder().setHash(bazelMd5Hash).build())
                .getDigestFunction())
        .isEqualTo(DigestFunction.Value.MD5);
    assertThat(
            DigestUtil.forDigest(digest.toBuilder().setHash(bazelSha1Hash).build())
                .getDigestFunction())
        .isEqualTo(DigestFunction.Value.SHA1);
    assertThat(
            DigestUtil.forDigest(digest.toBuilder().setHash(bazelSha256Hash).build())
                .getDigestFunction())
        .isEqualTo(DigestFunction.Value.SHA256);
    assertThat(
            DigestUtil.forDigest(digest.toBuilder().setHash(bazelSha384Hash).build())
                .getDigestFunction())
        .isEqualTo(DigestFunction.Value.SHA384);
    assertThat(
            DigestUtil.forDigest(digest.toBuilder().setHash(bazelSha512Hash).build())
                .getDigestFunction())
        .isEqualTo(DigestFunction.Value.SHA512);
  }

  @Test
  public void newHashingOutputStreamComputes() throws IOException {
    DigestUtil digestUtil = DigestUtil.forHash("MD5");
    ByteString.Output out = ByteString.newOutput();
    HashingOutputStream hashOut = digestUtil.newHashingOutputStream(out);
    bazelContent.writeTo(hashOut);
    hashOut.close();
    assertThat(hashOut.hash().toString()).isEqualTo(bazelMd5Hash);
    assertThat(out.toByteString()).isEqualTo(bazelContent);
  }

  @Test
  public void computePathMatches() throws IOException {
    Path root =
        Iterables.getFirst(
            Jimfs.newFileSystem(
                    Configuration.unix().toBuilder()
                        .setAttributeViews("basic", "owner", "posix", "unix")
                        .build())
                .getRootDirectories(),
            null);
    Path bazelPath = root.resolve("bazel");
    try (OutputStream out = Files.newOutputStream(bazelPath)) {
      bazelContent.writeTo(out);
    }
    DigestUtil digestUtil = DigestUtil.forHash("MD5");
    assertThat(digestUtil.compute(bazelPath))
        .isEqualTo(digestUtil.build(bazelMd5Hash, bazelContent.size()));
  }
}
