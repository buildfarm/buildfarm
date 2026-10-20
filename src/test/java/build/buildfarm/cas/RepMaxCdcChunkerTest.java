// Copyright 2026 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.cas;

import static com.google.common.truth.Truth.assertThat;

import build.buildfarm.cas.ContentAddressableStorage.Blob;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import com.google.common.io.ByteStreams;
import com.google.protobuf.ByteString;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class RepMaxCdcChunkerTest {
  private static final int MIN_SIZE = 4096;
  private static final int HORIZON_SIZE = 32768;
  private static final DigestUtil DIGEST_UTIL = new DigestUtil(HashFunction.SHA256);
  private static final List<Integer> EXPECTED_SIZES =
      List.of(
          7026, 5198, 5310, 4271, 5278, 5588, 5017, 6761, 5541, 7612, 5501, 4695, 4362, 6221, 7190,
          5858, 4382, 8022, 5633);
  private static final List<String> EXPECTED_HASHES =
      List.of(
          "3c386537b8c3200dd2ab6623f6096a576d07844a4a0d5d2994d13e24887dae46",
          "cc7fb08694b471e3ba660b700b347775d1e7a55db8f308c176e2eb6776b5222c",
          "096a3e0e42f0a8b7a5a3a337e9b5b4c7fc59abcd6f69486fb68fe4c5c156300f",
          "3f4a0b00f9a04959806c9ce9e530fa5d761930ef6c8477117b58eb93196342e8",
          "d0c2f82664d8887bd2d289a75baea1d5688e6ee586f8767149bfef9c6df8fe63",
          "558e6b58c659f7463c846a0d18c155a86520d1ca69102f102635c08e8b0897e2",
          "c3c66478eb8e1632768e6e13dc31e492ac78befca38e9db85525b4b274c5b5d7",
          "a5ce22ca3a02323451ddca6db5319c26b5fc1abc5621a033c98cd3de154cd29d",
          "ec4752962eda083bf78b24cb087e07451ced85d8c0d719f32c474f7dae8fd764",
          "3d9e8dc0a5f14dee7db5eabf99bb636352081423f122a84fdb160cecc2ad01bb",
          "f8c747a292f7fd6433945356ee314b978b423ac4c3d248c8fdf36c0693eb6006",
          "71249e1031dc237ab6393381a30a10ef90d9a93f532c135b11cc0df60a767b5f",
          "95c42ec307be0ce1efde6de814c668f05a31887fde9501b478faca225cf9b8c7",
          "8869f4871a0f3fb63775e0fb7f10e170a59b71a6fa0a91a18fa3cf5fff987eba",
          "d2f91644457863f716071e76e9b1046408820aa95b36471d5e70ee5c91b611a2",
          "3507024624db4ff48ee546e984d7c60461d6a526aba7e4aa33833b162785e79e",
          "9b19e98bc5d9b6f486c7c7281fd61044b8e5333363048b20ce3dcb2ee0172980",
          "e56dbfa2a82695fed3c565857dbc644af91c983b55c13fe20d9e03bb89e009e2",
          "7d76fde9af9a911c4e580db9227e5109c5de28101f3f26637c527e80215aad38");

  @Test
  public void chunksGoReferenceVector() throws IOException {
    try (InputStream input =
        RepMaxCdcChunkerTest.class.getResourceAsStream("fastcdc2020/SekienAkashita.jpg.base64")) {
      assertThat(input).isNotNull();
      byte[] image = Base64.getMimeDecoder().decode(ByteStreams.toByteArray(input));
      assertThat(DIGEST_UTIL.compute(ByteString.copyFrom(image)).getHash())
          .isEqualTo("d9e749d9367fc908876749d6502eb212fee88c9a94892fb07da5ef3ba8bc39ed");
      assertChunks(new ByteArrayInputStream(image), EXPECTED_SIZES, EXPECTED_HASHES);
    }
  }

  @Test
  public void outputIsDeterministicBoundedAndIndependentOfReads() {
    byte[] data = new byte[10 * MIN_SIZE + 123];
    new Random(123).nextBytes(data);
    List<Blob> direct = chunks(new ByteArrayInputStream(data), MIN_SIZE, HORIZON_SIZE);
    List<Blob> refilled = chunks(new OneByteInputStream(data), MIN_SIZE, HORIZON_SIZE);
    assertThat(digests(direct)).isEqualTo(digests(refilled));
    assertThat(ByteString.copyFrom(data)).isEqualTo(join(direct));
    for (Blob blob : direct) {
      assertThat(blob.getDigest().getSize()).isAtLeast((long) MIN_SIZE);
      assertThat(blob.getDigest().getSize()).isLessThan((long) 2 * MIN_SIZE);
    }
  }

  @Test
  public void zeroHorizonProducesUniformChunks() {
    int minSize = 1024;
    byte[] data = new byte[5 * minSize + 23];
    new Random(42).nextBytes(data);

    List<Blob> chunks = chunks(new ByteArrayInputStream(data), minSize, 0);

    assertThat(chunks.stream().map(blob -> blob.getDigest().getSize()).toList())
        .containsExactly(1024L, 1024L, 1024L, 1024L, 1047L)
        .inOrder();
    assertThat(join(chunks)).isEqualTo(ByteString.copyFrom(data));
  }

  @Test
  public void handlesTerminalChunkSizes() {
    int minSize = 1024;
    for (int inputSize : List.of(0, 1, minSize - 1, minSize, 2 * minSize - 1, 2 * minSize)) {
      byte[] data = new byte[inputSize];
      new Random(inputSize).nextBytes(data);

      List<Blob> chunks = chunks(new ByteArrayInputStream(data), minSize, 8 * minSize);

      assertThat(join(chunks)).isEqualTo(ByteString.copyFrom(data));
      for (Blob chunk : chunks) {
        assertThat(chunk.getDigest().getSize()).isLessThan(2L * minSize);
      }
    }
  }

  @Test
  public void handlesLowEntropyAndRefills() {
    int minSize = 64;
    int horizonSize = 8 * minSize;
    byte[] data = new byte[20 * (2 * minSize + horizonSize) + 17];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) (i % 7);
    }

    List<Blob> chunks = chunks(new OneByteInputStream(data), minSize, horizonSize);

    assertThat(join(chunks)).isEqualTo(ByteString.copyFrom(data));
    assertThat(chunks.stream().map(blob -> blob.getDigest().getSize()).reduce(0L, Long::sum))
        .isEqualTo((long) data.length);
    for (Blob chunk : chunks) {
      assertThat(chunk.getDigest().getSize()).isAtLeast((long) minSize);
      assertThat(chunk.getDigest().getSize()).isLessThan(2L * minSize);
    }
  }

  private static void assertChunks(InputStream input, List<Integer> sizes, List<String> hashes) {
    List<Blob> chunks = chunks(input);
    assertThat(chunks.stream().map(blob -> (int) blob.getDigest().getSize()).toList())
        .isEqualTo(sizes);
    assertThat(chunks.stream().map(blob -> blob.getDigest().getHash()).toList()).isEqualTo(hashes);
  }

  private static List<Blob> chunks(InputStream input) {
    return chunks(input, MIN_SIZE, HORIZON_SIZE);
  }

  private static List<Blob> chunks(InputStream input, int minSize, int horizonSize) {
    Iterator<Blob> iterator = new RepMaxCdcChunker(DIGEST_UTIL, input, minSize, horizonSize);
    List<Blob> chunks = new ArrayList<>();
    while (iterator.hasNext()) chunks.add(iterator.next());
    return chunks;
  }

  private static ByteString join(List<Blob> chunks) {
    ByteString result = ByteString.EMPTY;
    for (Blob chunk : chunks) result = result.concat(chunk.getData());
    return result;
  }

  private static List<String> digests(List<Blob> chunks) {
    return chunks.stream().map(blob -> blob.getDigest().toString()).toList();
  }

  private static class OneByteInputStream extends ByteArrayInputStream {
    OneByteInputStream(byte[] data) {
      super(data);
    }

    @Override
    public synchronized int read(byte[] bytes, int offset, int length) {
      return super.read(bytes, offset, Math.min(length, 1));
    }
  }
}
