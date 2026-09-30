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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FastCDCChunkerTest {
  private static final int AVERAGE_CHUNK_SIZE = 16 * 1024;
  private static final int NORMALIZATION = 2;
  private static final String EXPECTED_IMAGE_HASH =
      "d9e749d9367fc908876749d6502eb212fee88c9a94892fb07da5ef3ba8bc39ed";

  // Values published in remote-apis' fastcdc2020_test_vectors.txt for seed 0.
  private static final List<Integer> EXPECTED_CHUNK_SIZES =
      List.of(19186, 19279, 17354, 16387, 19940, 17320);
  private static final List<String> EXPECTED_CHUNK_HASHES =
      List.of(
          "0f9efa589121d5d9e9e2c4ace91337d77cae866537143f6f15a0ffd525a77c2d",
          "c7c86a165573c16448cda35c9169742e85645af42be22889f8b96b8ee0ec7cb0",
          "bc88521e28a8b4479cdea5f75aa721a24f3a0a7d0be903aa6d505c574e51e89d",
          "4b8dac2652e4685c629d2bb1ae9d4448e676b86f2e67ca0b2fff3d9580184b79",
          "c0a7062da6f2386c28e086ee0cedd5732252741269838773cff1ddb05b2df6ed",
          "7fa5b12134dc75cd2ac8dc60d3a8f3c8d22f0ee9d4cf74a4aa937e2a0d2d79a5");

  @Test
  public void chunksPublishedFastCdc2020Vector() throws IOException {
    List<Integer> actualChunkSizes = new ArrayList<>();
    List<String> actualChunkHashes = new ArrayList<>();
    try (InputStream input =
        FastCDCChunkerTest.class.getResourceAsStream("fastcdc2020/SekienAkashita.jpg.base64")) {
      assertThat(input).isNotNull();
      byte[] image = Base64.getMimeDecoder().decode(ByteStreams.toByteArray(input));
      assertThat(new DigestUtil(HashFunction.SHA256).compute(ByteString.copyFrom(image)).getHash())
          .isEqualTo(EXPECTED_IMAGE_HASH);
      Iterator<Blob> chunks =
          new FastCDCChunker(
              new DigestUtil(HashFunction.SHA256),
              new ByteArrayInputStream(image),
              AVERAGE_CHUNK_SIZE,
              NORMALIZATION);
      while (chunks.hasNext()) {
        Blob chunk = chunks.next();
        actualChunkSizes.add((int) chunk.getDigest().getSize());
        actualChunkHashes.add(chunk.getDigest().getHash());
      }
    }

    assertThat(actualChunkSizes).containsExactlyElementsIn(EXPECTED_CHUNK_SIZES).inOrder();
    assertThat(actualChunkHashes).containsExactlyElementsIn(EXPECTED_CHUNK_HASHES).inOrder();
  }
}
