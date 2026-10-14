// Copyright 2025 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.instance.shard;

import build.buildfarm.common.function.IOSupplier;
import build.buildfarm.common.io.FeedbackOutputStream;
import io.grpc.Status;
import io.grpc.Status.Code;
import java.io.IOException;

class ReadOnlyAwareOutputStream extends FeedbackOutputStream {
  @FunctionalInterface
  private interface OnStream {
    void accept(FeedbackOutputStream out) throws IOException;
  }

  private final IOSupplier<FeedbackOutputStream> supplier;
  private FeedbackOutputStream feedbackOut;

  ReadOnlyAwareOutputStream(IOSupplier<FeedbackOutputStream> supplier) throws IOException {
    this(supplier, supplier.get());
  }

  private ReadOnlyAwareOutputStream(
      IOSupplier<FeedbackOutputStream> supplier, FeedbackOutputStream feedbackOut) {
    super();
    this.feedbackOut = feedbackOut;
    this.supplier = supplier;
  }

  private synchronized FeedbackOutputStream getOutput() throws IOException {
    if (feedbackOut == null) {
      feedbackOut = supplier.get();
    }
    return feedbackOut;
  }

  private void withReadOnlyRetry(OnStream onStream) throws IOException {
    // may be overeager here - need more metrics as well
    for (; ; ) {
      try {
        onStream.accept(getOutput());
        return;
      } catch (IOException e) {
        if (Status.fromThrowable(e).getCode() != Code.FAILED_PRECONDITION) {
          throw e;
        }
        feedbackOut = null;
      }
    }
  }

  @Override
  public void write(int b) throws IOException {
    withReadOnlyRetry(out -> out.write(b));
  }

  @Override
  public void write(byte[] b) throws IOException {
    withReadOnlyRetry(out -> out.write(b));
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    withReadOnlyRetry(out -> out.write(b, off, len));
  }

  @Override
  public void flush() throws IOException {
    withReadOnlyRetry(out -> out.flush());
  }

  @Override
  public void close() throws IOException {
    withReadOnlyRetry(out -> out.close());
  }

  @Override
  public boolean isReady() {
    return feedbackOut.isReady();
  }
}
