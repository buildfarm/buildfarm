// Copyright 2024 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.instance.stub;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import build.buildfarm.instance.stub.Chunker.Chunk;
import com.google.protobuf.ByteString;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ChunkerTest {
  // Drains a Chunker into the list of chunks it produces.
  //
  // Note the Chunker's iteration contract is inverted from a typical Iterator: hasNext() returns
  // true only once the data source is fully consumed (it is the signal that the chunk just
  // returned was the last one), and next() must be called before hasNext() is meaningful.
  private static List<Chunk> drain(Chunker chunker) throws IOException {
    List<Chunk> chunks = new ArrayList<>();
    do {
      chunks.add(chunker.next());
    } while (!chunker.hasNext());
    return chunks;
  }

  @Test
  public void freshChunkerHasNextReturnsFalseBeforeFirstNext() {
    Chunker chunker = Chunker.builder().setInput(new byte[] {1, 2, 3}).build();
    // Not yet initialized: hasNext() is false until the first next() opens the data source.
    assertThat(chunker.hasNext()).isFalse();
  }

  @Test
  public void singleChunkFitsWhenSmallerThanChunkSize() throws IOException {
    byte[] data = {1, 2, 3, 4, 5};
    Chunker chunker = Chunker.builder().setInput(data).setChunkSize(16).build();

    Chunk chunk = chunker.next();

    assertThat(chunk.getOffset()).isEqualTo(0);
    assertThat(chunk.getData()).isEqualTo(ByteString.copyFrom(data));
    // After consuming the only chunk, hasNext() reports completion.
    assertThat(chunker.hasNext()).isTrue();
  }

  @Test
  public void splitsInputIntoChunkSizedPieces() throws IOException {
    byte[] data = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    Chunker chunker = Chunker.builder().setInput(data).setChunkSize(4).build();

    List<Chunk> chunks = drain(chunker);

    assertThat(chunks).hasSize(3);
    assertThat(chunks.get(0).getOffset()).isEqualTo(0);
    assertThat(chunks.get(0).getData()).isEqualTo(ByteString.copyFrom(new byte[] {1, 2, 3, 4}));
    assertThat(chunks.get(1).getOffset()).isEqualTo(4);
    assertThat(chunks.get(1).getData()).isEqualTo(ByteString.copyFrom(new byte[] {5, 6, 7, 8}));
    assertThat(chunks.get(2).getOffset()).isEqualTo(8);
    assertThat(chunks.get(2).getData()).isEqualTo(ByteString.copyFrom(new byte[] {9, 10}));
  }

  @Test
  public void exactMultipleOfChunkSizeProducesNoEmptyTrailingChunk() throws IOException {
    byte[] data = {1, 2, 3, 4, 5, 6, 7, 8};
    Chunker chunker = Chunker.builder().setInput(data).setChunkSize(4).build();

    List<Chunk> chunks = drain(chunker);

    assertThat(chunks).hasSize(2);
    assertThat(chunks.get(0).getOffset()).isEqualTo(0);
    assertThat(chunks.get(1).getOffset()).isEqualTo(4);
  }

  @Test
  public void emptyInputYieldsSingleEmptyChunk() throws IOException {
    Chunker chunker = Chunker.builder().setInput(new byte[0]).build();

    Chunk chunk = chunker.next();

    assertThat(chunk.getOffset()).isEqualTo(0);
    assertThat(chunk.getData()).isEqualTo(ByteString.EMPTY);
    // The empty-input path resets rather than marking consumption, so hasNext() stays false.
    assertThat(chunker.hasNext()).isFalse();
  }

  @Test
  public void getSizeReflectsInput() {
    Chunker chunker = Chunker.builder().setInput(new byte[] {1, 2, 3, 4, 5, 6, 7}).build();
    assertThat(chunker.getSize()).isEqualTo(7);
  }

  @Test
  public void setInputFromByteStringMatchesByteArray() throws IOException {
    ByteString data = ByteString.copyFromUtf8("hello world");
    Chunker chunker = Chunker.builder().setInput(data).setChunkSize(16).build();

    Chunk chunk = chunker.next();

    assertThat(chunk.getData()).isEqualTo(data);
    assertThat(chunker.getSize()).isEqualTo(data.size());
  }

  @Test
  public void setInputFromInputStreamUsesProvidedSize() throws IOException {
    byte[] data = {9, 8, 7};
    InputStream in = new ByteArrayInputStream(data);
    Chunker chunker = Chunker.builder().setInput(data.length, in).setChunkSize(16).build();

    Chunk chunk = chunker.next();

    assertThat(chunk.getData()).isEqualTo(ByteString.copyFrom(data));
  }

  @Test
  public void resetReturnsToUninitializedState() throws IOException {
    byte[] data = {1, 2, 3, 4, 5, 6};
    Chunker chunker = Chunker.builder().setInput(data).setChunkSize(2).build();

    chunker.next(); // consume first chunk, offset advances to 2
    assertThat(chunker.getOffset()).isEqualTo(2);

    chunker.reset();

    assertThat(chunker.getOffset()).isEqualTo(0);
    assertThat(chunker.hasNext()).isFalse();
    // After reset the data source is re-read from the beginning.
    assertThat(chunker.next().getData()).isEqualTo(ByteString.copyFrom(new byte[] {1, 2}));
  }

  @Test
  public void seekBackwardResetsAndSkipsToOffset() throws IOException {
    byte[] data = {0, 1, 2, 3, 4, 5, 6, 7};
    Chunker chunker = Chunker.builder().setInput(data).setChunkSize(2).build();

    // advance through two chunks (offset -> 4)
    chunker.next();
    chunker.next();
    assertThat(chunker.getOffset()).isEqualTo(4);

    // seek backward to offset 2; this resets and skips forward to 2
    chunker.seek(2);
    assertThat(chunker.getOffset()).isEqualTo(2);

    Chunk chunk = chunker.next();
    assertThat(chunk.getOffset()).isEqualTo(2);
    assertThat(chunk.getData()).isEqualTo(ByteString.copyFrom(new byte[] {2, 3}));
  }

  @Test
  public void seekToZeroOnFreshChunkerIsNoOp() throws IOException {
    Chunker chunker = Chunker.builder().setInput(new byte[] {1, 2, 3}).setChunkSize(2).build();

    chunker.seek(0);

    assertThat(chunker.getOffset()).isEqualTo(0);
    assertThat(chunker.next().getOffset()).isEqualTo(0);
  }

  @Test
  public void nextThrowsWhenCalledAfterFullConsumption() throws IOException {
    Chunker chunker = Chunker.builder().setInput(new byte[] {1, 2}).setChunkSize(2).build();

    chunker.next(); // single chunk consumed, hasNext() now true
    assertThat(chunker.hasNext()).isTrue();

    // next() throws once hasNext() reports the source is exhausted.
    assertThrows(NoSuchElementException.class, chunker::next);
  }

  @Test
  public void chunkEqualsAndHashCode() {
    Chunker first = Chunker.builder().setInput(new byte[] {1, 2, 3}).build();
    Chunker second = Chunker.builder().setInput(new byte[] {1, 2, 3}).build();

    Chunk a;
    Chunk b;
    try {
      a = first.next();
      b = second.next();
    } catch (IOException e) {
      throw new AssertionError(e);
    }

    assertThat(a).isEqualTo(b);
    assertThat(a.hashCode()).isEqualTo(b.hashCode());
    assertThat(a).isNotEqualTo("not a chunk");
  }
}
