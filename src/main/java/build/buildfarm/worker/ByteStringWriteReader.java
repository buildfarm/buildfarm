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

package build.buildfarm.worker;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;

import build.buildfarm.common.Write;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ByteStringWriteReader implements Runnable {
  private final InputStream input;
  private final Write write;
  private ByteString.Output data = ByteString.newOutput();
  private boolean completed;
  private IOException exception = null;

  public ByteStringWriteReader(InputStream input, Write write) {
    this.input = input;
    this.write = write;
    completed = false;
  }

  public void run() {
    boolean closed = false;
    byte[] buffer = new byte[1024 * 16];
    int len;
    write.addListener(this::complete, directExecutor());
    try (OutputStream writeOut = write.getOutput(1, SECONDS)) {
      while (!isComplete() && (len = input.read(buffer)) != -1) {
        if (len != 0) {
          data.write(buffer, 0, len);
          writeOut.write(buffer, 0, len);
        }
      }
    } catch(IOException e) {
      exception = e;
    } finally {
      try {
        input.close();
      } catch(IOException e) {
        if (exception == null) {
          exception = e;
        } else {
          exception.addSuppressed(e);
        }
      }
    }
  }

  private synchronized void waitForComplete() throws InterruptedException {
    while (!completed) {
      wait();
    }
  }

  private synchronized void complete() {
    completed = true;
    notify();
  }

  public synchronized boolean isComplete() {
    return completed;
  }

  public ByteString getData() throws IOException, InterruptedException {
    if (exception != null) {
      throw exception;
    }
    waitForComplete();
    return data.toByteString();
  }
}
