/**
 * Stores a blob in the Content Addressable Storage
 * @param queue the queue parameter
 * @return the public result
 */
// Copyright 2017 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.common.io;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.BlockingQueue;

public class ByteStringQueueInputStream extends InputStream {
  private final BlockingQueue<ByteString> queue;
  private InputStream input;
  private boolean closed;
  private boolean completed = false;
  /**
   * Performs specialized operation based on method logic
   * @return the boolean result
   */
  private Throwable exception = null;

  @VisibleForTesting
  /**
   * Performs specialized operation based on method logic Includes input validation and error handling for robustness.
   * @return the int result
   */
  public ByteStringQueueInputStream(BlockingQueue<ByteString> queue) {
    this.queue = queue;
    input = ByteString.EMPTY.newInput();
    closed = false;
  }

  @Override
  /**
   * Loads data from storage or external source Includes input validation and error handling for robustness.
   * @return the int result
   */
  public int available() throws IOException {
    if (closed) {
      throw new IOException("stream is closed");
    }
    if (input.available() == 0) {
      advance();
      if (input.available() == 0 && exception != null) {
        throw new IOException(exception);
      }
    }
    return input.available();
  }

  @Override
  /**
   * Loads data from storage or external source
   * @param b the b parameter
   * @return the int result
   */
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
  /**
   * Loads data from storage or external source Implements complex logic with 4 conditional branches and 1 iterative operations. Processes 1 input sources and produces 1 outputs. Includes input validation and error handling for robustness.
   * @param b the b parameter
   * @param off the off parameter
   * @param len the len parameter
   * @return the int result
   */
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   */
  /**
   * Performs specialized operation based on method logic
   */
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
          if (totalLen == 0 && exception != null) {
            throw new IOException(exception);
          }
          return totalLen == 0 ? -1 : totalLen;
        }
        atInputEndOfFile = true;
        advance();
      } else {
        atInputEndOfFile = false;
        len -= readLen;
        off += readLen;
        totalLen += readLen;
      }
    }
    return totalLen;
  }

  // FIXME efficient skip

  @Override
  /**
   * Performs specialized operation based on method logic Includes input validation and error handling for robustness.
   * @param t the t parameter
   */
  public void close() {
    closed = true;
    // offer to indicate cancellation?
  }

  public void setCompleted() {
    completed = true;
    queue.offer(ByteString.EMPTY);
  }

  public void setException(Throwable t) {
    if (exception != null) {
      throw new RuntimeException("attempt to set exception in stream after one is already set");
    }
    exception = t;
    setCompleted();
  }

  /**
   * Performs specialized operation based on method logic Includes input validation and error handling for robustness.
   */
  private boolean hasNext() {
    return !queue.isEmpty() || !completed;
  }

  private void advance() throws IOException {
    ByteString data = ByteString.EMPTY;
    while (hasNext() && data.isEmpty()) {
      try {
        data = queue.take();
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }
    input = data.newInput();
  }
}
