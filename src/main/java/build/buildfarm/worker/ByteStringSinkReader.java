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

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ByteStringSinkReader implements Runnable {
  private final InputStream input;
  private final OutputStream sink;
  private ByteString data;
  private boolean completed;

  public ByteStringSinkReader(InputStream input, OutputStream sink) {
    this.input = input;
    this.sink = sink;
    data = ByteString.EMPTY;
    completed = false;
  }

  public void run() {
    boolean closed = false;
    byte[] buffer = new byte[1024 * 16];
    int len;
    try {
      while (!isComplete() && (len = input.read(buffer, 0, buffer.length)) != -1) {
        if (len != 0) {
          data = data.concat(ByteString.copyFrom(buffer, 0, len));
          sink.write(buffer, 0, len);
        }
      }
    } catch(IOException ex) {
    } finally {
      try { input.close(); } catch(IOException ex) { }
      try { sink.close(); } catch(IOException ex) { }
      complete();
    }
  }

  private synchronized void complete() {
    completed = true;
  }

  public synchronized boolean isComplete() {
    return completed; 
  }

  public ByteString getData() {
    if (!isComplete()) {
      throw new IllegalStateException("cannot retrieve data while reader is running");
    }
    return data;
  }
}

