package build.buildfarm.cas;

import build.buildfarm.cas.ContentAddressableStorage.Blob;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import com.google.common.hash.HashCode;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class FastCDCChunker implements Iterator<Blob> {
  private static final com.google.common.hash.HashFunction MD5 = HashFunction.MD5.getHash();
  private static final int SEED_LENGTH = 64;
  public static final Long[] GEAR = generateGear();
  public static final Long[] GEAR_LS = generateGearLS();

  private static final long[] MASKS = {
    0, // 0: padding
    0, // 1: padding
    0, // 2: padding
    0, // 3: padding
    0, // 4: padding
    0x0000000001804110L, // 5: unused except for NC 3
    0x0000000001803110L, // 6: 64B
    0x0000000018035100L, // 7: 128B
    0x0000001800035300L, // 8: 256B
    0x0000019000353000L, // 9: 512B
    0x0000590003530000L, // 10: 1KB
    0x0000d90003530000L, // 11: 2KB
    0x0000d90103530000L, // 12: 4KB
    0x0000d90303530000L, // 13: 8KB
    0x0000d90313530000L, // 14: 16KB
    0x0000d90f03530000L, // 15: 32KB
    0x0000d90303537000L, // 16: 64KB
    0x0000d90703537000L, // 17: 128KB
    0x0000d90707537000L, // 18: 256KB
    0x0000d91707537000L, // 19: 512KB
    0x0000d91747537000L, // 20: 1MB
    0x0000d91767537000L, // 21: 2MB
    0x0000d93767537000L, // 22: 4MB
    0x0000d93777537000L, // 23: 8MB
    0x0000d93777577000L, // 24: 16MB
    0x0000db3777577000L, // 25: unused except for NC 3
  };

  // above is power of 2, important for our fast check
  //
  // The minimum and maximum chunk sizes MUST be derived from the average:
  //   - min_chunk_size = avg_chunk_size_bytes / 4
  //   - max_chunk_size = avg_chunk_size_bytes * 4
  private final int averageSize;
  private final int minSize;
  private final int maxSize;
  // masking relies on pow2
  private final long maskStrict;
  private final long maskLoose;
  private final long maskStrictLS;
  private final long maskLooseLS;

  private final DigestUtil digestUtil;
  private final InputStream in;
  private ByteString chunk = ByteString.EMPTY;

  private static HashCode hashByte(int i) {
    byte[] seed = new byte[SEED_LENGTH];
    Arrays.fill(seed, (byte) i);
    return MD5.newHasher().putBytes(seed).hash();
  }

  private static long high64Bits(HashCode hash) {
    byte[] bytes = hash.asBytes();
    // HashCode::asLong looks appealing, but reverses the endianness of the result
    // GearTable: 3b5d3c7d207e37dc => asLong: dc377e207d3c5d3b
    return ByteBuffer.wrap(bytes, 0, 8).getLong();
  }

  private static Stream<Long> gearStream() {
    // GEAR table: 256 64-bit integers for the rolling hash, computed as:
    //   GEAR[i] = high_64_bits(MD5(byte[SEED_LENGTH].fill(i))) for i in 0..255
    return IntStream.range(0, 256)
        .mapToObj(FastCDCChunker::hashByte)
        .map(FastCDCChunker::high64Bits);
  }

  private static Long[] generateGear() {
    return gearStream().toArray(Long[]::new);
  }

  private static Long[] generateGearLS() {
    return gearStream().map(n -> n << 1).toArray(Long[]::new);
  }

  public FastCDCChunker(DigestUtil digestUtil, InputStream in, int averageSize, int normalization) {
    this.digestUtil = digestUtil;
    this.in = in;
    this.averageSize = averageSize;

    int bits = 31 - Integer.numberOfLeadingZeros(averageSize);
    int smallBits = bits + normalization;
    int largeBits = bits - normalization;

    minSize = averageSize / (1 << normalization);
    maxSize = averageSize * (1 << normalization);
    maskStrict = MASKS[smallBits];
    maskLoose = MASKS[largeBits];
    maskStrictLS = maskStrict << 1;
    maskLooseLS = maskLoose << 1;
  }

  @Override
  public boolean hasNext() {
    if (chunk.size() < maxSize) {
      chunk = chunk.concat(nextChunk());
    }
    return !chunk.isEmpty();
  }

  @Override
  public Blob next() {
    int n = findChunkBoundary();
    Blob blob = new Blob(chunk.substring(0, n), digestUtil);
    chunk = chunk.substring(n);
    return blob;
  }

  private ByteString nextChunk() {
    byte[] buf = new byte[maxSize];
    try {
      int len = in.read(buf);
      if (len == -1) {
        return ByteString.EMPTY;
      }
      return ByteString.copyFrom(buf, 0, len);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private int findChunkBoundary() {
    int size = chunk.size();
    int avgLimit = Math.min(size, averageSize);
    int maxLimit = Math.min(size, maxSize);
    long hash = 0;
    int position = minSize;
    while (position < avgLimit) {
      // position incremented during loop to make it 2 bytes

      // &-away signedness
      int data = chunk.byteAt(position) & 0xff;
      hash = (hash << 2) + GEAR_LS[data];

      position++; // preincrement for check
      if ((hash & maskStrictLS) == 0 || position == size) {
        return position - 1; // size of run
      }

      // &-away signedness
      data = chunk.byteAt(position) & 0xff;
      hash = hash + GEAR[data];

      if ((hash & maskStrict) == 0) {
        return position; // size of run
      }
      position++;
    }

    while (position < maxLimit) {
      // position incremented during loop to make it 2 bytes

      // &-away signedness
      int data = chunk.byteAt(position) & 0xff;
      hash = (hash << 2) + GEAR_LS[data];

      position++; // preincrement for check
      if ((hash & maskLooseLS) == 0 || position == size) {
        return position - 1; // size of run
      }

      // &-away signedness
      data = chunk.byteAt(position) & 0xff;
      hash = hash + GEAR[data];

      if ((hash & maskLoose) == 0) {
        return position; // size of run
      }
      position++;
    }

    return maxLimit;
  }
}
