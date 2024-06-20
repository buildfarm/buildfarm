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
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

public class ByteStringWriteReader implements Runnable {
  private final InputStream input;
  private final Write write;
  private final ByteString.Output data = ByteString.newOutput();
  private boolean completed;
  private IOException exception = null;

  private final int dataLimit;

  public ByteStringWriteReader(InputStream input, Write write, int dataLimit) {
    this.input = input;
    this.write = write;
    this.dataLimit = dataLimit;
    Preconditions.checkState(dataLimit > 0);
    completed = false;
  }

  @Override
  public void run() {
    byte[] buffer = new byte[1024 * 16];
    int len;
    write.getFuture().addListener(this::complete, directExecutor());
    // may want to buffer this if not ready
    try (OutputStream writeOut = write.getOutput(1, SECONDS, () -> {})) {
      while (!isComplete() && (len = input.read(buffer)) != -1) {
        if (len != 0) {
          int dataLen = len;
          int size = data.size();
          if (size < dataLimit) {
            if (size + dataLen > dataLimit) {
              dataLen = dataLimit - size;
            }
            data.write(buffer, 0, len);
            if (size + dataLen > dataLimit) {
              new OutputStreamWriter(data).write("\nOutput truncated after exceeding limit.\n");
            }
          }
          writeOut.write(buffer, 0, len);
        }
      }
    } catch (IOException e) {
      // ignore asynchronous stream closure, this fulfills our objective
      // openjdk throws with the following copy when the process stderr/out
      // streams are asynchronously closed on process termination.
      if (!e.getMessage().equals("Stream closed")) {
        exception = e;
      }
    } finally {
      try {
        input.close();
      } catch (IOException e) {
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
