// Copyright 2018 The Bazel Authors. All rights reserved.
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

package build.buildfarm.worker;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

final class Downloader {
  private static final int BUFSIZE = 16 * 1024;

  private Downloader() {
  }

  @FunctionalInterface
  public static interface OffsetInputStreamFactory {
    InputStream newInputAt(long offset) throws IOException, InterruptedException;
  }

  public static final void copy(
      OffsetInputStreamFactory factory,
      OutputStream out) throws IOException, InterruptedException {
    byte[] buf = new byte[BUFSIZE];
    long offset = 0;

    for (;;) {
      long startOffset = offset;
      try (InputStream in = factory.newInputAt(offset)) {
        for (;;) {
          int bytesToRead = in.available();
          if (bytesToRead == 0 || bytesToRead > buf.length) {
            bytesToRead = buf.length;
          }
          int bytesRead = in.read(buf, 0, bytesToRead);
          if (bytesRead < 0) {
            return;
          }
          if (bytesRead > 0) {
            out.write(buf, 0, bytesRead);
          }
          offset += bytesRead;
        }
      } catch (IOException e) {
        Status status = Status.fromThrowable(e);
        if (status.getCode() != Status.Code.DEADLINE_EXCEEDED
            || startOffset == offset) {
          throw e;
        }
      }
    }
  }
}
