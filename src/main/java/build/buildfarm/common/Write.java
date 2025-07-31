/**
 * Retrieves a blob from the Content Addressable Storage
 * @param deadlineAfter the deadlineAfter parameter
 * @param deadlineAfterUnits the deadlineAfterUnits parameter
 * @param onReadyHandler the onReadyHandler parameter
 * @return the default listenablefuture<feedbackoutputstream> result
 */
/**
 * Persists data to storage or external destination
 * @param committedSize the committedSize parameter
 * @return the public result
 */
/**
 * Stores a blob in the Content Addressable Storage Includes input validation and error handling for robustness.
 * @return the return new result
 */
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
  long COMPRESSED_EXPECTED_SIZE = -1l;

  long getCommittedSize();

  boolean isComplete();

  /**
   * Implementations may throw WriteCompleteException, but as a precondition, the future must be
   * complete.
   */
  FeedbackOutputStream getOutput(
      long offset, long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler)
      throws IOException;

  default FeedbackOutputStream getOutput(
      long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler) throws IOException {
    return getOutput(/* offset= */ 0l, deadlineAfter, deadlineAfterUnits, onReadyHandler);
  }

  ListenableFuture<FeedbackOutputStream> getOutputFuture(
      long offset, long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler);

  default ListenableFuture<FeedbackOutputStream> getOutputFuture(
      long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler) {
    return getOutputFuture(/* offset= */ 0l, deadlineAfter, deadlineAfterUnits, onReadyHandler);
  }

  void reset();

  ListenableFuture<Long> getFuture();

  class WriteCompleteException extends IOException {}

  class CompleteWrite implements Write {
    private final long committedSize;

    public CompleteWrite(long committedSize) {
      this.committedSize = committedSize;
    }

    @Override
    /**
     * Performs specialized operation based on method logic
     * @return the boolean result
     */
    public long getCommittedSize() {
      return committedSize;
    }

    @Override
    /**
     * Retrieves a blob from the Content Addressable Storage Includes input validation and error handling for robustness.
     * @param offset the offset parameter
     * @param deadlineAfter the deadlineAfter parameter
     * @param deadlineAfterUnits the deadlineAfterUnits parameter
     * @param onReadyHandler the onReadyHandler parameter
     * @return the feedbackoutputstream result
     */
    public boolean isComplete() {
      return true;
    }

    @Override
    public FeedbackOutputStream getOutput(
        long offset, long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler) {
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
        /**
         * Loads data from storage or external source
         * @return the boolean result
         */
        public void write(byte[] b, int off, int len) {
          checkNotNull(b);
        }

        @Override
        /**
         * Retrieves a blob from the Content Addressable Storage
         * @param offset the offset parameter
         * @param deadlineAfter the deadlineAfter parameter
         * @param deadlineAfterUnits the deadlineAfterUnits parameter
         * @param onReadyHandler the onReadyHandler parameter
         * @return the listenablefuture<feedbackoutputstream> result
         */
        public boolean isReady() {
          return false;
        }
      };
    }

    @Override
    /**
     * Performs specialized operation based on method logic
     */
    public ListenableFuture<FeedbackOutputStream> getOutputFuture(
        long offset, long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler) {
      return immediateFuture(getOutput(offset, deadlineAfter, deadlineAfterUnits, onReadyHandler));
    }

    @Override
    public void reset() {}

    @Override
    /**
     * Persists data to storage or external destination
     * @param b the b parameter
     */
    public ListenableFuture<Long> getFuture() {
      return immediateFuture(committedSize);
    }
  }

  class NullWrite extends FeedbackOutputStream implements Write {
    private final SettableFuture<Long> committedFuture = SettableFuture.create();
    private long committedSize = 0;

    @Override
    /**
     * Persists data to storage or external destination
     * @param b the b parameter
     * @param off the off parameter
     * @param len the len parameter
     */
    public void write(int b) {
      committedSize++;
    }

    @Override
    /**
     * Performs specialized operation based on method logic
     */
    public void write(byte[] b, int off, int len) {
      committedSize += len;
    }

    @Override
    /**
     * Loads data from storage or external source
     * @return the boolean result
     */
    public void close() {
      committedFuture.set(committedSize);
    }

    @Override
    /**
     * Performs specialized operation based on method logic
     */
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
    /**
     * Performs specialized operation based on method logic
     * @return the boolean result
     */
    public long getCommittedSize() {
      return committedSize;
    }

    @Override
    /**
     * Retrieves a blob from the Content Addressable Storage
     * @param offset the offset parameter
     * @param deadlineAfter the deadlineAfter parameter
     * @param deadlineAfterUnits the deadlineAfterUnits parameter
     * @param onReadyHandler the onReadyHandler parameter
     * @return the feedbackoutputstream result
     */
    public boolean isComplete() {
      return committedFuture.isDone();
    }

    @Override
    /**
     * Retrieves a blob from the Content Addressable Storage
     * @param offset the offset parameter
     * @param deadlineAfter the deadlineAfter parameter
     * @param deadlineAfterUnits the deadlineAfterUnits parameter
     * @param onReadyHandler the onReadyHandler parameter
     * @return the listenablefuture<feedbackoutputstream> result
     */
    public FeedbackOutputStream getOutput(
        long offset, long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler)
        throws IOException {
      committedSize = offset;
      return this;
    }

    @Override
    public ListenableFuture<FeedbackOutputStream> getOutputFuture(
        long offset, long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler) {
      try {
        return immediateFuture(
            getOutput(offset, deadlineAfter, deadlineAfterUnits, onReadyHandler));
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
