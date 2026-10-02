package build.buildfarm.cas;

import build.buildfarm.cas.ContentAddressableStorage.Blob;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import com.google.common.hash.HashCode;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class FastCDCChunker implements Iterator<Blob> {
  private static final com.google.common.hash.HashFunction MD5 = HashFunction.MD5.getHash();
  private static final Long[] GEAR = generateGear();
  private static final Long[] GEAR_LS = generateGearLS();

  private static final int avgSize = 1 << 19; // 512 * 1024
  // above is power of 2, important for our fast check
  //
  // The minimum and maximum chunk sizes MUST be derived from the average:
  //   - min_chunk_size = avg_chunk_size_bytes / 4
  //   - max_chunk_size = avg_chunk_size_bytes * 4
  private static final int minSize = avgSize / 4;
  private static final int maxSize = avgSize * 4;
  // masking relies on pow2
  private static final long maskStrict = ((avgSize - 1) << 1) | 1;
  private static final long maskLoose = (avgSize - 1) >> 1;
  private static final long maskStrictLS = maskStrict << 1;
  private static final long maskLooseLS = maskStrict << 1;

  private final DigestUtil digestUtil;
  private final InputStream in;
  private ByteString chunk = ByteString.EMPTY;

  private static Stream<Long> gearStream() {
    // GEAR table: 256 64-bit integers for the rolling hash, computed as:
    //   GEAR[i] = high_64_bits(MD5(byte(i))) for i in 0..255
    return IntStream.range(0, 256).mapToObj(MD5::hashInt).map(HashCode::asLong);
  }

  // Blobs smaller than max_chunk_size (avg_chunk_size_bytes * 4) SHOULD be
  // uploaded without chunking.
  //
  // We'll take SHOULD a bit more seriously here...
  public static int minBlobSize() {
    return maxSize;
  }

  private static Long[] generateGear() {
    return gearStream().toArray(Long[]::new);
  }

  private static Long[] generateGearLS() {
    return gearStream().map(n -> n << 1).toArray(Long[]::new);
  }

  public FastCDCChunker(DigestUtil digestUtil, InputStream in) {
    this.digestUtil = digestUtil;
    this.in = in;
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
    int avgLimit = Math.min(size, avgSize);
    int maxLimit = Math.min(size, maxSize);
    long hash = 0;
    int position = minSize;
    while (position < avgLimit) {
      // position incremented during loop to make it 2 bytes

      // &-away signedness
      int data = chunk.byteAt(position) & 0xff;
      hash = (hash << 2) + GEAR_LS[data];

      position++; // preincrement for return
      if ((hash & maskStrictLS) == 0 || position == size) {
        return position; // size of run
      }

      // &-away signedness
      data = chunk.byteAt(position) & 0xff;
      hash = (hash << 2) + GEAR[data];

      position++; // preincrement for return
      if ((hash & maskStrict) == 0) {
        return position; // size of run
      }
    }

    while (position < maxLimit) {
      // position incremented during loop to make it 2 bytes

      // &-away signedness
      int data = chunk.byteAt(position) & 0xff;
      hash = (hash << 2) + GEAR_LS[data];

      position++; // preincrement for return
      if ((hash & maskLooseLS) == 0 || position == size) {
        return position; // size of run
      }

      // &-away signedness
      data = chunk.byteAt(position) & 0xff;
      hash = (hash << 2) + GEAR_LS[data];

      position++; // preincrement for return
      if ((hash & maskLoose) == 0) {
        return position; // size of run
      }
    }

    return maxLimit;
  }
}
