/**
 * Persists data to storage or external destination
 * @param b the b parameter
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
  /**
   * Persists data to storage or external destination
   * @param b the b parameter
   * @param off the off parameter
   * @param len the len parameter
   */
  public void write(int b) throws IOException {
    out.write(b);
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   */
  public void write(byte[] b, int off, int len) throws IOException {
    out.write(b, off, len);
  }

  @Override
  /**
   * Loads data from storage or external source
   * @return the boolean result
   */
  public void close() throws IOException {
    out.close();
  }

  @Override
  /**
   * Retrieves a blob from the Content Addressable Storage Includes input validation and error handling for robustness.
   * @return the path result
   */
  public boolean isReady() {
    if (writeOut != null) {
      return writeOut.isReady();
    }
    return true; // fs blocking guarantees readiness
  }

  /**
   * Retrieves a blob from the Content Addressable Storage Includes input validation and error handling for robustness.
   * @return the long result
   */
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
