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
import com.google.protobuf.ByteString;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class RepMaxCDCChunkerTest {
  private static final int MIN_SIZE = 256 * 1024;
  private static final DigestUtil DIGEST_UTIL = new DigestUtil(HashFunction.SHA256);

  @Test
  public void chunksAndPreservesInput() {
    byte[] data = new byte[10 * MIN_SIZE + 123];
    new Random(123).nextBytes(data);

    List<Blob> chunks = chunks(data);

    assertThat(join(chunks)).isEqualTo(ByteString.copyFrom(data));
    for (Blob chunk : chunks) {
      assertThat(chunk.getDigest().getSize()).isAtLeast((long) MIN_SIZE);
      assertThat(chunk.getDigest().getSize()).isLessThan(2L * MIN_SIZE);
    }
  }

  @Test
  public void preservesSmallTerminalInput() {
    byte[] data = new byte[MIN_SIZE - 1];
    new Random(42).nextBytes(data);

    List<Blob> chunks = chunks(data);

    assertThat(join(chunks)).isEqualTo(ByteString.copyFrom(data));
    assertThat(chunks).hasSize(1);
  }

  @Test
  public void hasNextIsIdempotent() {
    byte[] data = new byte[2 * MIN_SIZE];
    new Random(7).nextBytes(data);

    Iterator<Blob> iterator = new RepMaxCDCChunker(DIGEST_UTIL, new ByteArrayInputStream(data));

    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.hasNext()).isTrue();
    Blob firstChunk = iterator.next();
    assertThat(firstChunk.getData()).isEqualTo(chunks(data).get(0).getData());
  }

  @Test
  public void emptyInputHasNoChunks() {
    Iterator<Blob> iterator =
        new RepMaxCDCChunker(DIGEST_UTIL, new ByteArrayInputStream(new byte[0]));

    assertThat(iterator.hasNext()).isFalse();
  }

  @Test(expected = NoSuchElementException.class)
  public void nextAfterEndThrows() {
    Iterator<Blob> iterator =
        new RepMaxCDCChunker(DIGEST_UTIL, new ByteArrayInputStream(new byte[0]));

    iterator.next();
  }

  private static List<Blob> chunks(byte[] data) {
    Iterator<Blob> iterator = new RepMaxCDCChunker(DIGEST_UTIL, new ByteArrayInputStream(data));
    List<Blob> chunks = new ArrayList<>();
    while (iterator.hasNext()) {
      chunks.add(iterator.next());
    }
    return chunks;
  }

  private static ByteString join(List<Blob> chunks) {
    ByteString result = ByteString.EMPTY;
    for (Blob chunk : chunks) {
      result = result.concat(chunk.getData());
    }
    return result;
  }
}
