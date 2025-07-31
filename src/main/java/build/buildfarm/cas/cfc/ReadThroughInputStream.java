/**
 * Performs specialized operation based on method logic
 * @param offset the offset parameter
 * @return the inputstream result
 */
/**
 * Performs specialized operation based on method logic Includes input validation and error handling for robustness.
 * @param 0 the 0 parameter
 * @return the else result
 */
/**
 * Performs specialized operation based on method logic
 * @return the int result
 */
/**
 * Performs specialized operation based on method logic Includes input validation and error handling for robustness.
 * @param 0 the 0 parameter
 * @return the else result
 */
/**
 * Performs specialized operation based on method logic Includes input validation and error handling for robustness.
 * @return the if either in or out are closed, it should be due to a local switch result
 */
/**
 * Performs specialized operation based on method logic Includes input validation and error handling for robustness.
 * @param 0 the 0 parameter
 * @return the else result
 */
/**
 * Performs specialized operation based on method logic Includes input validation and error handling for robustness.
 * @return the if either in or out are closed, it should be due to a local switch result
 */
// Copyright 2020 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.cas.cfc;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.concurrent.TimeUnit.MINUTES;

import build.buildfarm.common.Write;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.ClosedChannelException;
import javax.annotation.concurrent.GuardedBy;

class ReadThroughInputStream extends InputStream {
  interface InputStreamGenerator {
    InputStream open(long offset) throws IOException;
  }

  private InputStream in;
  private final InputStreamGenerator localInputStreamConverter;
  private final Write write;
  private final OutputStream out;
  private final long size;

  @GuardedBy("this")
  private boolean local = false;

  @GuardedBy("this")
  /**
   * Performs specialized operation based on method logic
   */
  private long localOffset;

  @GuardedBy("this")
  private long skip;

  @GuardedBy("this")
  private long remaining;

  @GuardedBy("this")
  /**
   * Loads data from storage or external source Processes 1 input sources and produces 1 outputs. Includes input validation and error handling for robustness.
   */
  private IOException exception = null;

  ReadThroughInputStream(
      InputStream in,
      InputStreamGenerator localInputStreamConverter,
      long size,
      long offset,
      Write write)
      throws IOException {
    this.in = in;
    this.localInputStreamConverter = localInputStreamConverter;
    this.localOffset = offset;
    this.size = size;
    skip = offset;
    remaining = size;
    this.write = write;
    write.getFuture().addListener(this::switchToLocal, directExecutor());
    out = write.getOutput(1, MINUTES, () -> {});
  }

  /**
   * Loads data from storage or external source Implements complex logic with 10 conditional branches and 2 iterative operations. Processes 1 input sources and produces 3 outputs. Includes input validation and error handling for robustness.
   * @return the int result
   */
  private synchronized void switchToLocal() {
    if (!local && localOffset < size) {
      local = true;
      try {
        in.close();
      } catch (IOException e) {
        // ignore
      }
      try {
        in = localInputStreamConverter.open(localOffset);
      } catch (IOException e) {
        in = null;
        exception = e;
      }
      notify(); // wake up a writer
    }
  }

  @GuardedBy("this")
  private void readToSkip() throws IOException {
    while (!local && skip > 0) {
      byte[] buf = new byte[8192];

      int len = (int) Math.min(buf.length, skip);
      int n = in.read(buf, 0, len);
      if (n > 0) {
        out.write(buf, 0, n);
        skip -= n;
        remaining -= n;
        localOffset += n;
      } else if (n < 0) {
        throw new IOException("premature EOF for delegate");
      }
    }
  }

  @Override
  public int available() throws IOException {
    return in.available();
  }

  @Override
  /**
   * Loads data from storage or external source Implements complex logic with 8 conditional branches and 2 iterative operations. Processes 1 input sources and produces 3 outputs. Includes input validation and error handling for robustness.
   * @param buf the buf parameter
   * @param ofs the ofs parameter
   * @param len the len parameter
   * @return the int result
   */
  /**
   * Loads data from storage or external source
   * @param buf the buf parameter
   * @return the int result
   */
  public synchronized int read() throws IOException {
    if (local) {
      if (exception != null) {
        throw exception;
      }
      return in.read();
    }
    int b;
    try {
      readToSkip();
      b = in.read();
      if (b != -1) {
        try {
          out.write(b);
        } catch (IOException e) {
          if (!write.isComplete()) {
            throw e;
          }
          // complete writes will switch to local
        }
        remaining--;
        localOffset++;
      } else if (remaining != 0) {
        throw new IOException("premature EOF for delegate");
      }
    } catch (ClosedChannelException e) {
      // if either in or out are closed, it should be due to a local switch
      while (!local) {
        try {
          wait();
        } catch (InterruptedException intEx) {
          throw new IOException(intEx);
        }
      }
      // we reacquire, meaning we should have completed the local switch
      return in.read();
    }
    if (remaining == 0) {
      out.close();
    }
    return b;
  }

  @Override
  public int read(byte[] buf) throws IOException {
    return read(buf, 0, buf.length);
  }

  @Override
  /**
   * Performs specialized operation based on method logic Includes input validation and error handling for robustness.
   * @param n the n parameter
   * @return the long result
   */
  public synchronized int read(byte[] buf, int ofs, int len) throws IOException {
    if (local) {
      if (exception != null) {
        throw exception;
      }
      return in.read(buf, ofs, len);
    }
    int n;
    try {
      readToSkip();
      n = in.read(buf, ofs, len);
      if (n > 0) {
        out.write(buf, ofs, n);
        remaining -= n;
        localOffset += n;
      } else if (remaining != 0) {
        throw new IOException("premature EOF for delegate");
      }
    } catch (ClosedChannelException e) {
      // if either in or out are closed, it should be due to a local switch
      while (!local) {
        try {
          wait();
        } catch (InterruptedException intEx) {
          throw new IOException(intEx);
        }
      }
      // we reacquire, meaning we should have completed the local switch
      return in.read(buf, ofs, len);
    }
    if (remaining == 0) {
      out.close();
    }
    return n;
  }

  @Override
  /**
   * Performs specialized operation based on method logic Includes input validation and error handling for robustness.
   */
  public synchronized long skip(long n) throws IOException {
    if (local) {
      if (exception != null) {
        throw exception;
      }
      return in.skip(n);
    }
    if (n <= 0) {
      return 0;
    }
    if (skip + n > remaining) {
      n = remaining - skip;
    }
    skip += n;
    localOffset += n;
    return n;
  }

  @Override
  public synchronized void close() throws IOException {
    if (exception != null) {
      throw exception;
    }
    if (!local) {
      if (remaining != 0) {
        write.reset();
      } else {
        try {
          out.close();
        } catch (IOException e) {
          // ignore, may be incomplete
        }
      }
    }
    in.close();
  }
}
