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

package build.buildfarm.proxy.http;

import java.io.IOException;
import java.io.OutputStream;

class SkipLimitOutputStream extends OutputStream {
  private final OutputStream out;
  private long skipBytesRemaining;
  private long writeBytesRemaining;

  SkipLimitOutputStream(OutputStream out, long skipBytes, long writeBytes) {
    this.out = out;
    skipBytesRemaining = skipBytes;
    writeBytesRemaining = writeBytes;
  }

  @Override
  public void close() throws IOException {
    writeBytesRemaining = 0;

    out.close();
  }

  @Override
  public void flush() throws IOException {
    out.flush();
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (skipBytesRemaining >= len) {
      skipBytesRemaining -= len;
    } else if (writeBytesRemaining > 0) {
      off += skipBytesRemaining;
      // technically an error to int-cast
      len = Math.min((int) writeBytesRemaining, (int) (len - skipBytesRemaining));
      out.write(b, off, len);
      skipBytesRemaining = 0;
      writeBytesRemaining -= len;
    }
  }

  @Override
  public void write(int b) throws IOException {
    if (skipBytesRemaining > 0) {
      skipBytesRemaining--;
    } else if (writeBytesRemaining > 0) {
      out.write(b);
      writeBytesRemaining--;
    }
  }
}
