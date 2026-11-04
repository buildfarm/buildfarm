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

/** Common buffered-stream iterator mechanics for content-defined chunkers. */
abstract class BufferedChunker implements Iterator<Blob> {
  protected final byte[] buffer;
  protected int bufferStart;

  private final DigestUtil digestUtil;
  private final InputStream in;
  private int bufferEnd;
  private int pendingChunkSize = -1;
  private boolean eof;

  protected BufferedChunker(DigestUtil digestUtil, InputStream in, int bufferSize) {
    this.digestUtil = digestUtil;
    this.in = in;
    buffer = new byte[bufferSize];
  }

  @Override
  public final boolean hasNext() {
    if (pendingChunkSize == -1) {
      pendingChunkSize = nextChunkSize();
    }
    return pendingChunkSize != 0;
  }

  @Override
  public final Blob next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    int chunkSize = pendingChunkSize;
    pendingChunkSize = -1;
    Blob blob = new Blob(ByteString.copyFrom(buffer, bufferStart, chunkSize), digestUtil);
    bufferStart += chunkSize;
    return blob;
  }

  /** Returns up to {@code size} buffered bytes, filling the buffer as needed. */
  protected final int peek(int size) {
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

  /** Computes the next chunk size from the bytes currently at {@link #bufferStart}. */
  protected abstract int nextChunkSize();
}
