// Copyright 2019 The Bazel Authors. All rights reserved.
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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;

import build.buildfarm.common.io.FeedbackOutputStream;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

// still toying with the idea of just making a Write implement a ListenableFuture
public interface Write {
  long getCommittedSize();

  boolean isComplete();

  FeedbackOutputStream getOutput(
      long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler) throws IOException;

  ListenableFuture<FeedbackOutputStream> getOutputFuture(
      long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler);

  void reset();

  ListenableFuture<Long> getFuture();

  class WriteCompleteException extends IOException {}

  class CompleteWrite implements Write {
    private final long committedSize;

    public CompleteWrite(long committedSize) {
      this.committedSize = committedSize;
    }

    @Override
    public long getCommittedSize() {
      return committedSize;
    }

    @Override
    public boolean isComplete() {
      return true;
    }

    @Override
    public FeedbackOutputStream getOutput(
        long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler) {
      return new FeedbackOutputStream() {
        /** Discards the specified byte. */
        @Override
        public void write(int b) {}
        /** Discards the specified byte array. */
        @Override
        public void write(byte[] b) {
          checkNotNull(b);
        }

        /** Discards the specified byte array. */
        @Override
        public void write(byte[] b, int off, int len) {
          checkNotNull(b);
        }

        @Override
        public boolean isReady() {
          return false;
        }
      };
    }

    @Override
    public ListenableFuture<FeedbackOutputStream> getOutputFuture(
        long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler) {
      return immediateFuture(getOutput(deadlineAfter, deadlineAfterUnits, onReadyHandler));
    }

    @Override
    public void reset() {}

    @Override
    public ListenableFuture<Long> getFuture() {
      return immediateFuture(committedSize);
    }
  }

  class NullWrite extends FeedbackOutputStream implements Write {
    private final SettableFuture<Long> committedFuture = SettableFuture.create();
    private long committedSize = 0;

    @Override
    public void write(int b) {
      committedSize++;
    }

    @Override
    public void write(byte[] b, int off, int len) {
      committedSize += len;
    }

    @Override
    public void close() {
      committedFuture.set(committedSize);
    }

    @Override
    public boolean isReady() {
      return true;
    }

    // Write methods

    @Override
    public void reset() {
      // hopefully we are not closed...
      committedSize = 0;
    }

    @Override
    public long getCommittedSize() {
      return committedSize;
    }

    @Override
    public boolean isComplete() {
      return committedFuture.isDone();
    }

    @Override
    public FeedbackOutputStream getOutput(
        long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler)
        throws IOException {
      return this;
    }

    @Override
    public ListenableFuture<FeedbackOutputStream> getOutputFuture(
        long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler) {
      try {
        return immediateFuture(getOutput(deadlineAfter, deadlineAfterUnits, onReadyHandler));
      } catch (IOException e) {
        return immediateFailedFuture(e);
      }
    }

    @Override
    public ListenableFuture<Long> getFuture() {
      return committedFuture;
    }
  }
}
