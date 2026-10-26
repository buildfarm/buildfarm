// Copyright 2026 The Buildfarm Authors. All rights reserved.
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

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.String.format;

import build.buildfarm.common.function.IOSupplier;
import build.buildfarm.common.io.FeedbackOutputStream;
import build.buildfarm.v1test.Digest;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import lombok.extern.java.Log;

@Log
public final class WritesHelper {
  // REAPI servers may report -1 on ALREADY_EXISTS or compressed-upload responses
  private static final long UNKNOWN_COMMITTED_SIZE = -1L;

  private WritesHelper() {}

  /**
   * Stream an {@link InputStream} into a {@link Write} using gRPC's onReady pattern.
   *
   * <p>Returns a future resolving to the number of bytes uploaded. If the Write is already
   * complete, resolves directly from {@link Write#getFuture()} without opening the input.
   * Otherwise, this wires an {@link Uploader} into the Write as its onReady handler and primes it
   * once. gRPC drives subsequent firings as flow control allows.
   *
   * <p>For non-gRPC streams that initial onReady handler call writes the entire stream because
   * isReady is always true for those streams.
   */
  public static ListenableFuture<Long> streamIntoWriteFuture(
      IOSupplier<InputStream> inFactory,
      Write write,
      Digest digest,
      long deadlineAfter,
      TimeUnit deadlineAfterUnits) {
    SettableFuture<Long> writtenFuture = SettableFuture.create();

    boolean isComplete;
    try {
      isComplete = write.isComplete();
    } catch (RuntimeException e) {
      writtenFuture.setException(e);
      return writtenFuture;
    }

    if (isComplete) {
      resolveFromWriteFuture(write, digest, writtenFuture);
      return writtenFuture;
    }

    InputStream in;
    try {
      in = inFactory.get();
    } catch (IOException e) {
      writtenFuture.setException(e);
      return writtenFuture;
    }

    FeedbackOutputStream out;
    Uploader uploader = new Uploader(in, write, digest, writtenFuture);
    try {
      out = write.getOutput(deadlineAfter, deadlineAfterUnits, uploader);
    } catch (Write.WriteCompleteException e) {
      // TOCTOU race with isComplete() above. If we're here the Write's future is now complete.
      closeQuietly(in);
      resolveFromWriteFuture(write, digest, writtenFuture);
      return writtenFuture;
    } catch (IOException e) {
      // attachListeners() hasn't run yet, so close in directly.
      try {
        in.close();
      } catch (IOException suppressed) {
        e.addSuppressed(suppressed);
      }
      writtenFuture.setException(e);
      return writtenFuture;
    }

    uploader.out = out;
    uploader.attachListeners();
    // Make sure to fire run() once, so any of the non-gRPC streams write their bytes.
    // For gRPC producers this should harmlessly race gRPC's regular firing of its onReadyHandler.
    uploader.run();
    return writtenFuture;
  }

  /**
   * Propagate the completion of {@link Write#getFuture()} to {@code writtenFuture}: succeed with
   * {@code digest.getSize()} on success, or fail with the underlying cause.
   *
   * <p>Warns but does not fail on committed-size mismatch: REAPI servers can report inconsistent
   * committed_size on ALREADY_EXISTS, compressed uploads, or via intermediate proxies.
   */
  private static void resolveFromWriteFuture(
      Write write, Digest digest, SettableFuture<Long> writtenFuture) {
    try {
      long committedSize = write.getFuture().get();
      if (committedSize != digest.getSize() && committedSize != UNKNOWN_COMMITTED_SIZE) {
        log.log(
            Level.WARNING,
            format(
                "committed size %d did not match expectation for %s",
                committedSize, DigestUtil.toString(digest)));
      }
      writtenFuture.set(digest.getSize());
    } catch (ExecutionException e) {
      writtenFuture.setException(e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      writtenFuture.setException(e);
    } catch (RuntimeException e) {
      writtenFuture.setException(e);
    }
  }

  /** Close, ignoring IOException. Used before any listener is attached to surface close errors. */
  private static void closeQuietly(InputStream in) {
    try {
      in.close();
    } catch (IOException ignored) {
    }
  }

  /**
   * State and callbacks for one streaming upload. Plays three roles for the same {@link Write}:
   *
   * <ul>
   *   <li>{@link #run()}: the gRPC onReady handler that pumps bytes from {@code in} into {@code
   *       out}.
   *   <li>{@link #propagate()}: listener on {@code write.getFuture()} that resolves the user-facing
   *       future once the server commits.
   *   <li>{@link #cleanup()}: listener on {@code writtenFuture} that closes resources and cancels
   *       the Write on failure or cancellation.
   * </ul>
   */
  private static final class Uploader implements Runnable {
    // The Buildfarm code this was based on used a 128 KB buffer for years, so we're using that
    // same size buffer here.
    // Small-blob buffers are sized down to digest.getSize() to avoid over allocating memory.
    static final int MAX_COPY_BUF_BYTES = 128 * 1024;

    private final InputStream in;
    private final Write write;
    private final Digest digest;
    private final SettableFuture<Long> writtenFuture;
    private final byte[] copyBuf;
    private final Object uploadPhaseLock = new Object();

    // Must be set BEFORE out.close() is called at EOF. Calls to close() that complete getFuture()
    // inline cause the propagate listener to fire reentrantly, which sets the writtenFuture and
    // fires cleanup before close() returns.
    private boolean outClosedAtEof;

    // This is volatile because out is written by the caller thread after getOutput() returns but
    // read by run() invocations that gRPC may fire from its transport thread. Additionally, gRPC
    // may fire onReady inline during getOutput() before `out` is assigned. run() early-returns
    // when out is null, so the explicit run() after this is assigned is the first real invocation.
    private volatile FeedbackOutputStream out;

    Uploader(InputStream in, Write write, Digest digest, SettableFuture<Long> writtenFuture) {
      this.in = in;
      this.write = write;
      this.digest = digest;
      this.writtenFuture = writtenFuture;
      long size = digest.getSize();
      // If the blob is smaller than the default buffer size, avoid allocating unnecessary bytes
      int bufSize = MAX_COPY_BUF_BYTES;
      if (size >= 0 && size < MAX_COPY_BUF_BYTES) {
        bufSize = (int) size;
      }
      this.copyBuf = new byte[bufSize];
    }

    /**
     * gRPC onReady handler: copy from {@code in} into {@code out} while the output is ready. Closes
     * {@code out} on EOF.
     *
     * <p>Post-commit close IOExceptions are suppressed because CASFileCache and
     * MemoryWriteOutputStream commit inside {@link FeedbackOutputStream#close()}.
     */
    @Override
    public void run() {
      if (out == null) {
        return;
      }

      synchronized (uploadPhaseLock) {
        try {
          while (out.isReady()) {
            // If the future is done, we're done. Exit early.
            if (writtenFuture.isDone()) {
              return;
            }

            int n = ByteStreams.read(in, copyBuf, 0, copyBuf.length);
            if (n == 0) {
              outClosedAtEof = true;

              try {
                out.close();
              } catch (IOException e) {
                // CASFileCache and MemoryWriteOutputStream commit inside close() and we don't want
                // to override the successful commit in those cases.
                if (!write.isComplete()) {
                  writtenFuture.setException(e);
                }
              }
              return;
            }
            out.write(copyBuf, 0, n);
          }
        } catch (IOException e) {
          if (!write.isComplete()) {
            // cleanup() fires reentrantly via directExecutor on setException and calls
            // write.cancel, so we don't need to cancel here.
            writtenFuture.setException(e);
          }
        }
      }
    }

    /**
     * Wire the Uploader as listener on both {@code write.getFuture()} and {@code writtenFuture}.
     */
    private void attachListeners() {
      write.getFuture().addListener(this::propagate, directExecutor());
      writtenFuture.addListener(this::cleanup, directExecutor());
    }

    /**
     * Listener on {@code write.getFuture()} that resolves the user-facing future on server commit.
     */
    private void propagate() {
      resolveFromWriteFuture(write, digest, writtenFuture);
    }

    /**
     * Listener on {@code writtenFuture}: cancels the Write on failure or caller cancel, then closes
     * {@code out} and {@code in}.
     */
    private void cleanup() {
      if (writtenFuture.isCancelled()) {
        write.cancel("upload cancelled by caller", null);
      } else {
        // On the exceptional path, tear the Write down so native resources are released rather
        // than relying on out.close() finding the call already broken.
        try {
          writtenFuture.get();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
          write.cancel("upload failed", null);
        }
      }

      // Hold uploadPhaseLock so this cleanup doesn't race with an in-flight run().
      synchronized (uploadPhaseLock) {
        if (!outClosedAtEof) {
          try {
            out.close();
          } catch (IOException ignored) {
          }
        }
        try {
          in.close();
        } catch (IOException ignored) {
        }
      }
    }
  }
}
