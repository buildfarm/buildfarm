// Copyright 2017 The Bazel Authors. All rights reserved.
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

package build.buildfarm.instance.stub;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

public class ByteStringIteratorInputStream extends InputStream {
  private final Iterator<ByteString> iterator;
  private final Retrier retrier;
  private InputStream input;
  private boolean closed;

  @VisibleForTesting
  public ByteStringIteratorInputStream(Iterator<ByteString> iterator, Retrier retrier) {
    this.iterator = iterator;
    this.retrier = retrier;
    input = ByteString.EMPTY.newInput();
    closed = false;
  }

  @Override
  public int available() throws IOException {
    if (closed) {
      throw new IOException("stream is closed");
    }
    if (input.available() == 0) {
      advance();
    }
    return input.available();
  }

  @Override
  public int read() throws IOException {
    if (closed) {
      throw new IOException("stream is closed");
    }
    byte[] b = new byte[1];
    if (read(b) == -1) {
      return -1;
    }
    return b[0];
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (closed) {
      throw new IOException("stream is closed");
    }
    boolean atInputEndOfFile = false;
    int totalLen = 0;
    while (len != 0) {
      int readLen = input.read(b, off, len);
      if (readLen == -1) {
        if (atInputEndOfFile) {
          return totalLen == 0 ? -1 : totalLen;
        }
        atInputEndOfFile = true;
        advance();
        continue; // restart read with EOF indicator
      }
      atInputEndOfFile = false;
      len -= readLen;
      off += readLen;
      totalLen += readLen;
    }
    return totalLen;
  }

  // FIXME efficient skip

  @Override
  public void close() {
    closed = true;
  }

  private void advance() throws IOException {
    try {
      retrier.execute(() -> {
        ByteString data = ByteString.EMPTY;
        while (iterator.hasNext() && data.isEmpty()) {
          data = iterator.next();
        }
        input = data.newInput();
        return 0;
      });
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
