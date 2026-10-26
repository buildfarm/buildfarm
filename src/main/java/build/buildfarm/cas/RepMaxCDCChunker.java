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

import build.buildfarm.cas.ContentAddressableStorage.Blob;
import build.buildfarm.common.DigestUtil;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Splits a stream using RepMax content-defined chunking.
 *
 * <p>This is a port of Buildbarn's optimized implementation. It preserves rolling-hash and cut
 * candidate state across chunks, so every input byte is hashed only once.
 */
public class RepMaxCDCChunker implements Iterator<Blob> {
  private static final int GEAR_HASH_WINDOW_SIZE = 64;
  private static final int MIN_SIZE = 256 * 1024;
  private static final int HORIZON_SIZE = 8 * MIN_SIZE;
  private static final long[] GEAR =
      java.util.Arrays.stream(FastCDCChunker.GEAR).mapToLong(Long::longValue).toArray();

  private final int minSize;
  private final int peekSize;
  private final DigestUtil digestUtil;
  private final InputStream in;
  private final byte[] buffer;
  private final IntList completeChunks = new IntList();
  private final IntList incompleteChunks = new IntList();
  private int bufferStart;
  private int bufferEnd;
  private int pendingChunkSize = -1;
  private int previousChunkSize;
  private long currentHash;
  private long bestHash;
  private boolean eof;

  public RepMaxCDCChunker(DigestUtil digestUtil, InputStream in) {
    this.minSize = MIN_SIZE;
    peekSize = 2 * MIN_SIZE + HORIZON_SIZE;
    this.digestUtil = digestUtil;
    this.in = in;
    buffer = new byte[peekSize * 2];
  }

  @Override
  public boolean hasNext() {
    if (pendingChunkSize == -1) {
      pendingChunkSize = nextChunkSize();
    }
    return pendingChunkSize != 0;
  }

  @Override
  public Blob next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    int chunkSize = pendingChunkSize;
    pendingChunkSize = -1;
    Blob blob = new Blob(ByteString.copyFrom(buffer, bufferStart, chunkSize), digestUtil);
    previousChunkSize = chunkSize;
    return blob;
  }

  private int peek(int size) {
    int available = bufferEnd - bufferStart;
    if (available < size && !eof) {
      if (bufferStart > 0 && available > 0) {
        System.arraycopy(buffer, bufferStart, buffer, 0, available);
      }
      bufferStart = 0;
      bufferEnd = available;
      while (bufferEnd < buffer.length) {
        try {
          int read = in.read(buffer, bufferEnd, buffer.length - bufferEnd);
          if (read == -1) {
            eof = true;
            break;
          }
          if (read > 0) {
            bufferEnd += read;
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      available = bufferEnd - bufferStart;
    }
    return Math.min(size, available);
  }

  // This compatibility port intentionally keeps Buildbarn's state-machine structure.
  @SuppressWarnings({"PMD.CognitiveComplexity", "PMD.ForLoopVariableCount"})
  private int nextChunkSize() {
    bufferStart += previousChunkSize;
    previousChunkSize = 0;

    if (!completeChunks.isEmpty()) {
      return completeChunks.popLast();
    }

    int dataLength = peek(peekSize);
    if (dataLength < 2 * minSize) {
      return dataLength;
    }
    dataLength -= minSize;
    int base = bufferStart;

    int currentChunk;
    long hash;
    long best;
    if (incompleteChunks.size() >= 2) {
      currentChunk = incompleteChunks.popLast();
      hash = currentHash;
      best = bestHash;
    } else {
      incompleteChunks.clear();
      incompleteChunks.add(0);
      hash = 0;
      for (int i = minSize - GEAR_HASH_WINDOW_SIZE; i < minSize; i++) {
        hash = (hash << 1) + GEAR[buffer[base + i] & 0xff];
      }
      best = hash;
      currentChunk = 0;
    }

    int position = minSize + currentChunk;
    while (true) {
      int hashLength = dataLength - position;
      int originalChunkCount = -1;
      int bytesBeforeMinimum = incompleteChunks.last() + minSize - 1 - currentChunk;
      if (hashLength > bytesBeforeMinimum) {
        hashLength = bytesBeforeMinimum;
        originalChunkCount = incompleteChunks.size();
      } else if (hashLength == 0) {
        break;
      }

      for (int i = 0; i < hashLength; i++) {
        hash = (hash << 1) + GEAR[buffer[base + position + i] & 0xff];
        if (Long.compareUnsigned(best, hash) < 0) {
          best = hash;
          incompleteChunks.add(currentChunk + i + 1);
        }
      }

      if (incompleteChunks.size() == originalChunkCount) {
        int completedBefore = completeChunks.size();
        int nextChunk = incompleteChunks.last();
        for (int i = incompleteChunks.size() - 3; nextChunk >= minSize; i--) {
          int chunk = incompleteChunks.get(i);
          if (nextChunk - chunk >= minSize) {
            completeChunks.add(nextChunk - chunk);
            nextChunk = chunk;
            i--;
          }
        }
        completeChunks.add(minSize + nextChunk);
        completeChunks.reverse(completedBefore, completeChunks.size());

        incompleteChunks.truncate(1);
        currentChunk = 0;
        hash = (hash << 1) + GEAR[buffer[base + position + hashLength] & 0xff];
        best = hash;
        position += hashLength + 1;
      } else {
        currentChunk += hashLength;
        position += hashLength;
      }
    }

    incompleteChunks.add(currentChunk);
    int firstChunk;
    if (!completeChunks.isEmpty()) {
      completeChunks.reverse(0, completeChunks.size());
      firstChunk = completeChunks.popLast();
    } else {
      int firstChunkIndex = incompleteChunks.size() - 2;
      for (int maxChunk = incompleteChunks.get(firstChunkIndex) - minSize, i = firstChunkIndex - 2;
          maxChunk >= 0;
          i--) {
        int chunk = incompleteChunks.get(i);
        if (chunk <= maxChunk) {
          firstChunkIndex = i;
          maxChunk = chunk - minSize;
          i--;
        }
      }
      firstChunk = minSize + incompleteChunks.get(firstChunkIndex);

      int reusableChunkIndex = firstChunkIndex + 1;
      while (true) {
        int offsetInSecondChunk = incompleteChunks.get(reusableChunkIndex) - firstChunk;
        if (offsetInSecondChunk >= 0) {
          for (int i = reusableChunkIndex; i < incompleteChunks.size(); i++) {
            incompleteChunks.set(i, incompleteChunks.get(i) - firstChunk);
          }

          if (offsetInSecondChunk == 0) {
            incompleteChunks.removePrefix(reusableChunkIndex);
          } else {
            int regionStart = base + firstChunk;
            long recomputedHash = 0;
            for (int i = minSize - GEAR_HASH_WINDOW_SIZE; i < minSize; i++) {
              recomputedHash = (recomputedHash << 1) + GEAR[buffer[regionStart + i] & 0xff];
            }
            incompleteChunks.set(0, 0);
            long bestRecomputedHash = recomputedHash;
            int recomputedChunkIndex = 1;
            int originalChunksCount = incompleteChunks.size();
            for (int i = 0; i < offsetInSecondChunk - 1; i++) {
              recomputedHash =
                  (recomputedHash << 1) + GEAR[buffer[regionStart + minSize + i] & 0xff];
              if (Long.compareUnsigned(bestRecomputedHash, recomputedHash) < 0) {
                bestRecomputedHash = recomputedHash;
                int recomputedChunk = i + 1;
                if (recomputedChunkIndex < reusableChunkIndex) {
                  incompleteChunks.set(recomputedChunkIndex++, recomputedChunk);
                } else {
                  incompleteChunks.add(recomputedChunk);
                }
              }
            }
            if (recomputedChunkIndex < reusableChunkIndex) {
              incompleteChunks.removeRange(recomputedChunkIndex, reusableChunkIndex);
            } else if (incompleteChunks.size() > originalChunksCount) {
              incompleteChunks.reverse(reusableChunkIndex, originalChunksCount);
              incompleteChunks.reverse(originalChunksCount, incompleteChunks.size());
              incompleteChunks.reverse(reusableChunkIndex, incompleteChunks.size());
            }
          }
          break;
        }
        if (++reusableChunkIndex == incompleteChunks.size()) {
          incompleteChunks.truncate(1);
          break;
        }
      }
    }
    currentHash = hash;
    bestHash = best;
    return firstChunk;
  }

  private static final class IntList {
    private int[] values = new int[32];
    private int size;

    int size() {
      return size;
    }

    boolean isEmpty() {
      return size == 0;
    }

    int get(int index) {
      return values[index];
    }

    void set(int index, int value) {
      values[index] = value;
    }

    int last() {
      return values[size - 1];
    }

    void add(int value) {
      if (size == values.length) {
        values = java.util.Arrays.copyOf(values, size * 2);
      }
      values[size++] = value;
    }

    int popLast() {
      return values[--size];
    }

    void clear() {
      size = 0;
    }

    void truncate(int newSize) {
      size = newSize;
    }

    void removePrefix(int count) {
      System.arraycopy(values, count, values, 0, size - count);
      size -= count;
    }

    void removeRange(int from, int to) {
      System.arraycopy(values, to, values, from, size - to);
      size -= to - from;
    }

    @SuppressWarnings("PMD.ForLoopVariableCount")
    void reverse(int from, int to) {
      for (int i = from, j = to - 1; i < j; i++, j--) {
        int value = values[i];
        values[i] = values[j];
        values[j] = value;
      }
    }
  }
}
