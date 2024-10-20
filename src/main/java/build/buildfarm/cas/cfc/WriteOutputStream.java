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

import build.buildfarm.common.io.FeedbackOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;

class WriteOutputStream extends FeedbackOutputStream {
  protected final OutputStream out;
  private final WriteOutputStream writeOut;

  WriteOutputStream(OutputStream out) {
    this.out = out;
    this.writeOut = null;
  }

  WriteOutputStream(WriteOutputStream writeOut) {
    this.out = writeOut;
    this.writeOut = writeOut;
  }

  @Override
  public void write(int b) throws IOException {
    out.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    out.write(b, off, len);
  }

  @Override
  public void close() throws IOException {
    out.close();
  }

  @Override
  public boolean isReady() {
    if (writeOut != null) {
      return writeOut.isReady();
    }
    return true; // fs blocking guarantees readiness
  }

  public Path getPath() {
    if (writeOut == null) {
      throw new UnsupportedOperationException();
    }
    return writeOut.getPath();
  }

  public long getWritten() {
    if (writeOut == null) {
      throw new UnsupportedOperationException();
    }
    return writeOut.getWritten();
  }
}
