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
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

/** Splits a stream using the RepMax content-defined chunking algorithm. */
public class RepMaxCdcChunker implements Iterator<Blob> {
  private static final int GEAR_HASH_WINDOW_SIZE = 64;

  private final int minSize;
  private final int peekSize;
  private final DigestUtil digestUtil;
  private final InputStream in;
  private final byte[] buffer;
  private int length;
  private boolean eof;

  public RepMaxCdcChunker(DigestUtil digestUtil, InputStream in, int minSize, int horizonSize) {
    Preconditions.checkArgument(minSize >= GEAR_HASH_WINDOW_SIZE, "minSize must be at least 64");
    Preconditions.checkArgument(horizonSize >= 0, "horizonSize must not be negative");
    Preconditions.checkArgument(
        2L * minSize + horizonSize <= Integer.MAX_VALUE / 2, "chunking buffer is too large");
    this.minSize = minSize;
    peekSize = 2 * minSize + horizonSize;
    this.digestUtil = digestUtil;
    this.in = in;
    buffer = new byte[peekSize];
  }

  @Override
  public boolean hasNext() {
    fill();
    return length > 0;
  }

  @Override
  public Blob next() {
    if (!hasNext()) {
      throw new java.util.NoSuchElementException();
    }
    int chunkSize = findChunkBoundary();
    Blob blob = new Blob(ByteString.copyFrom(buffer, 0, chunkSize), digestUtil);
    System.arraycopy(buffer, chunkSize, buffer, 0, length - chunkSize);
    length -= chunkSize;
    return blob;
  }

  private void fill() {
    while (!eof && length < peekSize) {
      try {
        int read = in.read(buffer, length, peekSize - length);
        if (read == -1) {
          eof = true;
        } else if (read > 0) {
          length += read;
        } else {
          // InputStream is permitted to return zero; retry without treating it as EOF.
          continue;
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  // This is the reference selection algorithm. The lookahead is deliberately bounded by peekSize
  // so stream refill boundaries cannot affect chunk boundaries.
  private int findChunkBoundary() {
    if (length < 2 * minSize) {
      return length;
    }

    int limit = length - minSize;
    long initialHash = 0;
    for (int i = minSize - GEAR_HASH_WINDOW_SIZE; i < minSize; i++) {
      initialHash = (initialHash << 1) + FastCDCChunker.GEAR[buffer[i] & 0xff];
    }
    while (true) {
      long hash = initialHash;
      long bestHash = hash;
      int bestEnd = minSize;
      for (int i = minSize; i < limit; i++) {
        hash = (hash << 1) + FastCDCChunker.GEAR[buffer[i] & 0xff];
        if (Long.compareUnsigned(bestHash, hash) < 0) {
          bestHash = hash;
          bestEnd = i + 1;
        }
      }
      if (bestEnd < 2 * minSize) {
        return bestEnd;
      }
      limit = bestEnd - minSize;
    }
  }
}
