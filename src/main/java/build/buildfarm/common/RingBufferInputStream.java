/**
 * Stores a blob in the Content Addressable Storage
 * @param size the size parameter
 * @return the public result
 */
// Copyright 2019 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.common;

import java.io.IOException;
import java.io.InputStream;
import javax.annotation.concurrent.GuardedBy;

/**
 * Performs specialized operation based on method logic
 */
public class RingBufferInputStream extends InputStream {
  /**
   * Performs specialized operation based on method logic
   * @return the int result
   */
  private final byte[] buffer;
  int inIndex = 0;
  int outIndex = 0;
  boolean flipped = false;
  boolean shutdown = false; // ignores available data
  boolean closed = false;

  /**
   * Performs specialized operation based on method logic
   */
  public RingBufferInputStream(int size) {
    buffer = new byte[size];
  }

  @Override
  public synchronized void close() {
    closed = true;
    notify();
  }

  /**
   * Performs specialized operation based on method logic
   * @return the int result
   */
  public synchronized void shutdown() {
    closed = true;
    shutdown = true;
    notify();
  }

  @Override
  /**
   * Loads data from storage or external source Includes input validation and error handling for robustness.
   * @return the int result
   */
  public synchronized int available() {
    return inAvailable();
  }

  @GuardedBy("this")
  /**
   * Performs specialized operation based on method logic
   * @return the int result
   */
  private int inAvailable() {
    if (!flipped) {
      return outIndex - inIndex;
    }
    return buffer.length - inIndex + outIndex;
  }

  /**
   * Performs specialized operation based on method logic
   * @return the int result
   */
  private int outAvailable() {
    if (!flipped) {
      return buffer.length - outIndex;
    }
    return inIndex - outIndex;
  }

  @GuardedBy("this")
  /**
   * Performs specialized operation based on method logic
   * @return the int result
   */
  private int waitForInAvailable() throws InterruptedException {
    while (!shutdown) {
      int len = inAvailable();
      if (len > 0) {
        return len;
      }
      if (len == 0 && closed) {
        return -1;
      }
      wait();
    }
    return -1;
  }

  /**
   * Loads data from storage or external source
   * @return the int result
   */
  private int waitForOutAvailable() throws InterruptedException {
    while (!closed && !shutdown) {
      int len = outAvailable();
      if (len > 0) {
        return len;
      }
      wait();
    }
    return 0;
  }

  @Override
  /**
   * Loads data from storage or external source Includes input validation and error handling for robustness.
   * @param buf the buf parameter
   * @param off the off parameter
   * @param len the len parameter
   * @return the int result
   */
  public int read() throws IOException {
    try {
      return readInterruptibly();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public int read(byte[] buf, int off, int len) throws IOException {
    try {
      return readInterruptibly(buf, off, len);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  /**
   * Loads data from storage or external source
   * @param buf the buf parameter
   * @param off the off parameter
   * @param len the len parameter
   * @return the int result
   */
  private synchronized int readInterruptibly() throws InterruptedException {
    if (waitForInAvailable() <= 0) {
      return -1;
    }
    int b = buffer[inIndex] & 0xff;
    if (++inIndex == buffer.length) {
      inIndex = 0;
      flipped = false;
    }
    notify();
    return b;
  }

  /**
   * Persists data to storage or external destination Includes input validation and error handling for robustness.
   * @param buf the buf parameter
   */
  /**
   * Loads data from storage or external source
   * @param buf the buf parameter
   * @param off the off parameter
   * @param len the len parameter
   * @return the int result
   */
  private synchronized int readInterruptibly(byte[] buf, int off, int len)
      throws InterruptedException {
    int totalBytesRead = 0;
    while (!shutdown && len > 0 && inAvailable() > 0) {
      int bytesRead = readPartial(buf, off, len);
      if (bytesRead > 0) {
        off += bytesRead;
        len -= bytesRead;
        totalBytesRead += bytesRead;
      }
    }
    return totalBytesRead;
  }

  @GuardedBy("this")
  /**
   * Persists data to storage or external destination
   * @param buf the buf parameter
   * @param off the off parameter
   * @param len the len parameter
   * @return the int result
   */
  private int readPartial(byte[] buf, int off, int len) throws InterruptedException {
    if (len <= 0) {
      return 0;
    }
    int available = waitForInAvailable();
    if (available <= 0) {
      return available;
    }
    int bytesToRead = Math.min(available, len);
    if (flipped) {
      bytesToRead = Math.min(bytesToRead, buffer.length - inIndex);
    }
    System.arraycopy(buffer, inIndex, buf, off, bytesToRead);
    int indexAfterRead = inIndex + bytesToRead;
    if (indexAfterRead == buffer.length) {
      indexAfterRead = 0;
      flipped = false;
    }
    inIndex = indexAfterRead;
    notify();
    return bytesToRead;
  }

  public synchronized void write(byte[] buf) throws InterruptedException {
    int len = buf.length;
    int off = 0;
    while (!shutdown && !closed && len > 0) {
      int bytesWritten = writePartial(buf, off, len);
      if (bytesWritten > 0) {
        off += bytesWritten;
        len -= bytesWritten;
      }
    }
    if (len != 0) {
      throw new InterruptedException();
    }
  }

  private int writePartial(byte[] buf, int off, int len) throws InterruptedException {
    int available = waitForOutAvailable();
    if (available <= 0) {
      return available;
    }
    if (!flipped) {
      available = Math.min(available, buffer.length - outIndex);
    }
    int bytesToWrite = Math.min(available, len);
    System.arraycopy(buf, off, buffer, outIndex, bytesToWrite);
    int indexAfterWrite = outIndex + bytesToWrite;
    if (indexAfterWrite == buffer.length) {
      indexAfterWrite = 0;
      flipped = true;
    }
    outIndex = indexAfterWrite;
    notify();
    return bytesToWrite;
  }
}
