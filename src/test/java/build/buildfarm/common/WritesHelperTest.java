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

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertThrows;

import build.bazel.remote.execution.v2.DigestFunction;
import build.buildfarm.common.Write.CompleteWrite;
import build.buildfarm.common.Write.WriteCompleteException;
import build.buildfarm.common.function.IOSupplier;
import build.buildfarm.common.io.FeedbackOutputStream;
import build.buildfarm.v1test.Digest;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class WritesHelperTest {
  private static Digest digestOf(long size) {
    return Digest.newBuilder()
        .setHash("test-hash")
        .setSize(size)
        .setDigestFunction(DigestFunction.Value.SHA256)
        .build();
  }

  /**
   * Fake Write whose backing output captures every chunk and whose getFuture() can be driven by the
   * test. isReady() returns true permanently — mimicking non-gRPC writes (CASFileCache, MemoryCAS)
   * where the helper's kickoff drains the entire InputStream in one pass.
   */
  private static final class CapturingWrite extends FeedbackOutputStream implements Write {
    final SettableFuture<Long> future = SettableFuture.create();
    final List<ByteString> chunks = new ArrayList<>();
    final AtomicInteger closeCount = new AtomicInteger();
    final AtomicInteger cancelCount = new AtomicInteger();
    boolean isReadyValue = true;

    CapturingWrite() {
      // Field initializers provide all the state needed to capture uploads.
    }

    @Override
    public boolean isReady() {
      return isReadyValue;
    }

    @Override
    public void write(int b) {
      chunks.add(ByteString.copyFrom(new byte[] {(byte) b}));
    }

    @Override
    public void write(byte[] b, int off, int len) {
      chunks.add(ByteString.copyFrom(b, off, len));
    }

    @Override
    public void close() {
      closeCount.incrementAndGet();
      // Complete the future on close — mirrors CASFileCache$UniqueWriteOutputStream and
      // MemoryWriteOutputStream behavior.
      long total = 0;
      for (ByteString chunk : chunks) {
        total += chunk.size();
      }
      future.set(total);
    }

    @Override
    public long getCommittedSize() {
      long total = 0;
      for (ByteString chunk : chunks) {
        total += chunk.size();
      }
      return total;
    }

    @Override
    public boolean isComplete() {
      return future.isDone();
    }

    @Override
    public FeedbackOutputStream getOutput(
        long offset, long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler) {
      return this;
    }

    @Override
    public ListenableFuture<FeedbackOutputStream> getOutputFuture(
        long offset, long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void reset() {}

    @Override
    public void cancel(String message, Throwable cause) {
      cancelCount.incrementAndGet();
    }

    @Override
    public ListenableFuture<Long> getFuture() {
      return future;
    }

    ByteString concatenated() {
      ByteString combined = ByteString.EMPTY;
      for (ByteString c : chunks) {
        combined = combined.concat(c);
      }
      return combined;
    }
  }

  /** InputStream wrapper that counts close() invocations. */
  private static final class CountingInputStream extends InputStream {
    final InputStream delegate;
    final AtomicInteger closeCount = new AtomicInteger();

    CountingInputStream(InputStream delegate) {
      this.delegate = delegate;
    }

    @Override
    public int read() throws IOException {
      return delegate.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return delegate.read(b, off, len);
    }

    @Override
    public void close() throws IOException {
      closeCount.incrementAndGet();
      delegate.close();
    }
  }

  @Test
  public void completeWrite_returnsImmediateFutureWithoutOpeningStream() throws Exception {
    AtomicInteger supplierInvocations = new AtomicInteger();
    IOSupplier<InputStream> throwingSupplier =
        () -> {
          supplierInvocations.incrementAndGet();
          throw new AssertionError("supplier must not be invoked when write isComplete");
        };

    ListenableFuture<Long> future =
        WritesHelper.streamIntoWriteFuture(
            throwingSupplier, new CompleteWrite(0), digestOf(0), 1, SECONDS);

    assertThat(future.isDone()).isTrue();
    assertThat(future.get()).isEqualTo(0);
    assertThat(supplierInvocations.get()).isEqualTo(0);
  }

  @Test
  public void earlyReturn_propagatesFailedFutureCause() throws Exception {
    IOException cause = new IOException("backing failure");
    Write alreadyFailed =
        new Write() {
          @Override
          public long getCommittedSize() {
            return 0;
          }

          @Override
          public boolean isComplete() {
            return true;
          }

          @Override
          public FeedbackOutputStream getOutput(long offset, long da, TimeUnit du, Runnable r) {
            throw new UnsupportedOperationException();
          }

          @Override
          public ListenableFuture<FeedbackOutputStream> getOutputFuture(
              long offset, long da, TimeUnit du, Runnable r) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void reset() {}

          @Override
          public ListenableFuture<Long> getFuture() {
            return immediateFailedFuture(cause);
          }
        };

    ListenableFuture<Long> future =
        WritesHelper.streamIntoWriteFuture(
            () -> new ByteArrayInputStream(new byte[0]), alreadyFailed, digestOf(0), 1, SECONDS);

    assertThat(future.isDone()).isTrue();
    ExecutionException ee = assertThrows(ExecutionException.class, future::get);
    assertThat(ee.getCause()).isSameInstanceAs(cause);
  }

  @Test
  public void nonGrpcWrite_drainsInputStreamSynchronouslyAtKickoff() throws Exception {
    // Payload deliberately exceeds the 128 KiB MAX_COPY_BUF_BYTES and is not a buffer-multiple, so
    // this also exercises the multi-iteration drain: two full-buffer reads plus a 7-byte partial
    // chunk before EOF.
    byte[] payload = new byte[2 * 128 * 1024 + 7];
    for (int i = 0; i < payload.length; i++) {
      payload[i] = (byte) i;
    }
    CapturingWrite capturing = new CapturingWrite();

    ListenableFuture<Long> future =
        WritesHelper.streamIntoWriteFuture(
            () -> new ByteArrayInputStream(payload),
            capturing,
            digestOf(payload.length),
            1,
            SECONDS);

    // For a write whose isReady() is permanently true, the kickoff drains the entire stream on
    // the calling thread across as many loop iterations as the buffer demands.
    assertThat(future.isDone()).isTrue();
    assertThat(future.get()).isEqualTo(payload.length);
    assertThat(capturing.concatenated()).isEqualTo(ByteString.copyFrom(payload));
    // 128 KiB + 128 KiB + 7 = three writes.
    assertThat(capturing.chunks).hasSize(3);
  }

  @Test
  public void nonGrpcWrite_closesOutAtEof_completingFuture() throws Exception {
    // The load-bearing test: for non-gRPC writes whose getFuture() completes only on close(), the
    // helper must close out at EOF or the writtenFuture would hang.
    byte[] payload = new byte[256];
    for (int i = 0; i < payload.length; i++) {
      payload[i] = (byte) i;
    }
    CapturingWrite capturing = new CapturingWrite();

    ListenableFuture<Long> future =
        WritesHelper.streamIntoWriteFuture(
            () -> new ByteArrayInputStream(payload),
            capturing,
            digestOf(payload.length),
            1,
            SECONDS);

    assertThat(future.isDone()).isTrue();
    assertThat(future.get()).isEqualTo(payload.length);
    // out.close() invoked exactly once (by run() at EOF, NOT again by cleanup).
    assertThat(capturing.closeCount.get()).isEqualTo(1);
  }

  @Test
  public void isReadyFlipping_pausesDrainBetweenReadyWindows() throws Exception {
    // Simulates gRPC backpressure: isReady() flips false after each write, parking the producer.
    // The captured onReadyHandler is re-fired by the test to simulate gRPC draining the outbound
    // queue and signalling readiness again. Verifies the helper drains one chunk per ready
    // window and only commits when EOF arrives on a ready iteration.
    byte[] payload = "Hello, World".getBytes();
    AtomicReference<Runnable> capturedHandler = new AtomicReference<>();
    AtomicBoolean isReadyFlag = new AtomicBoolean(true);
    AtomicInteger writeCount = new AtomicInteger();
    AtomicInteger closeCount = new AtomicInteger();
    SettableFuture<Long> writeFuture = SettableFuture.create();
    List<ByteString> chunks = new ArrayList<>();

    Write throttled =
        new Write() {
          @Override
          public long getCommittedSize() {
            return 0;
          }

          @Override
          public boolean isComplete() {
            return writeFuture.isDone();
          }

          @Override
          public FeedbackOutputStream getOutput(
              long offset, long da, TimeUnit du, Runnable onReadyHandler) {
            capturedHandler.set(onReadyHandler);
            return new FeedbackOutputStream() {
              @Override
              public boolean isReady() {
                return isReadyFlag.get();
              }

              @Override
              public void write(int b) {}

              @Override
              public void write(byte[] b, int off, int len) {
                chunks.add(ByteString.copyFrom(b, off, len));
                writeCount.incrementAndGet();
                // Simulate the queue filling up: producer parks after this write returns.
                isReadyFlag.set(false);
              }

              @Override
              public void close() {
                closeCount.incrementAndGet();
                writeFuture.set((long) payload.length);
              }
            };
          }

          @Override
          public ListenableFuture<FeedbackOutputStream> getOutputFuture(
              long offset, long da, TimeUnit du, Runnable r) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void reset() {}

          @Override
          public ListenableFuture<Long> getFuture() {
            return writeFuture;
          }
        };

    ListenableFuture<Long> writtenFuture =
        WritesHelper.streamIntoWriteFuture(
            () -> new ByteArrayInputStream(payload),
            throttled,
            digestOf(payload.length),
            1,
            SECONDS);

    // First ready window: kickoff did one write, then parked when isReady flipped false.
    assertThat(writeCount.get()).isEqualTo(1);
    assertThat(closeCount.get()).isEqualTo(0);
    assertThat(writtenFuture.isDone()).isFalse();

    // Simulate gRPC's queue draining and onReady firing again.
    isReadyFlag.set(true);
    capturedHandler.get().run();

    // Second ready window: producer sees EOF and closes out, completing the future.
    assertThat(writeCount.get()).isEqualTo(1);
    assertThat(closeCount.get()).isEqualTo(1);
    assertThat(writtenFuture.isDone()).isTrue();
    assertThat(writtenFuture.get()).isEqualTo(payload.length);
  }

  @Test
  public void cleanup_closesInputStreamExactlyOnceOnSuccess() throws Exception {
    byte[] payload = "data".getBytes();
    CountingInputStream in = new CountingInputStream(new ByteArrayInputStream(payload));
    CapturingWrite capturing = new CapturingWrite();

    ListenableFuture<Long> future =
        WritesHelper.streamIntoWriteFuture(
            () -> in, capturing, digestOf(payload.length), 1, SECONDS);

    assertThat(future.isDone()).isTrue();
    assertThat(in.closeCount.get()).isEqualTo(1);
  }

  @Test
  public void inputStreamFactoryThrows_propagatesSynchronouslyAndDoesNotCallCancel()
      throws Exception {
    IOException factoryFailure = new IOException("factory failed");
    AtomicInteger cancelCount = new AtomicInteger();
    Write neverInvoked =
        new Write() {
          @Override
          public long getCommittedSize() {
            return 0;
          }

          @Override
          public boolean isComplete() {
            return false;
          }

          @Override
          public FeedbackOutputStream getOutput(long offset, long da, TimeUnit du, Runnable r) {
            throw new AssertionError("getOutput must not be called when factory throws");
          }

          @Override
          public ListenableFuture<FeedbackOutputStream> getOutputFuture(
              long offset, long da, TimeUnit du, Runnable r) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void reset() {}

          @Override
          public void cancel(String message, Throwable cause) {
            cancelCount.incrementAndGet();
          }

          @Override
          public ListenableFuture<Long> getFuture() {
            return SettableFuture.create();
          }
        };

    ListenableFuture<Long> future =
        WritesHelper.streamIntoWriteFuture(
            () -> {
              throw factoryFailure;
            },
            neverInvoked,
            digestOf(10),
            1,
            SECONDS);
    assertThat(future.isDone()).isTrue();
    ExecutionException thrown = assertThrows(ExecutionException.class, future::get);
    assertThat(thrown).hasCauseThat().isSameInstanceAs(factoryFailure);
    // The helper has not yet committed to the upload — no cancel.
    assertThat(cancelCount.get()).isEqualTo(0);
  }

  @Test
  public void inputStreamReadThrows_callsWriteCancelAndPropagatesViaFuture() throws Exception {
    IOException readFailure = new IOException("read failed");
    InputStream throwingStream =
        new InputStream() {
          @Override
          public int read() throws IOException {
            throw readFailure;
          }

          @Override
          public int read(byte[] b, int off, int len) throws IOException {
            throw readFailure;
          }
        };

    CapturingWrite capturing = new CapturingWrite();
    ListenableFuture<Long> future =
        WritesHelper.streamIntoWriteFuture(
            () -> throwingStream, capturing, digestOf(100), 1, SECONDS);

    assertThat(future.isDone()).isTrue();
    ExecutionException ee = assertThrows(ExecutionException.class, future::get);
    assertThat(ee.getCause()).isSameInstanceAs(readFailure);
    // write.cancel was invoked when the producer body caught the IOException.
    assertThat(capturing.cancelCount.get()).isEqualTo(1);
  }

  @Test
  public void writeFutureFailedExternally_propagatesThroughWrittenFuture() throws Exception {
    // Simulates server-side onError before EOF: external completion of write.getFuture(). The
    // CapturingWrite test fake completes its future only on close(); for this test we use a
    // never-completing Write so the helper waits for our explicit setException.
    SettableFuture<Long> writeFuture = SettableFuture.create();
    AtomicInteger cancelCount = new AtomicInteger();
    Write external =
        new Write() {
          @Override
          public long getCommittedSize() {
            return 0;
          }

          @Override
          public boolean isComplete() {
            return writeFuture.isDone();
          }

          @Override
          public FeedbackOutputStream getOutput(
              long offset, long da, TimeUnit du, Runnable onReadyHandler) {
            // Return a stream whose isReady() flips false after first iteration; this lets the
            // helper park before any real I/O happens.
            return new FeedbackOutputStream() {
              @Override
              public boolean isReady() {
                return false;
              }

              @Override
              public void write(int b) {}
            };
          }

          @Override
          public ListenableFuture<FeedbackOutputStream> getOutputFuture(
              long offset, long da, TimeUnit du, Runnable r) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void reset() {}

          @Override
          public void cancel(String message, Throwable cause) {
            cancelCount.incrementAndGet();
          }

          @Override
          public ListenableFuture<Long> getFuture() {
            return writeFuture;
          }
        };

    ListenableFuture<Long> writtenFuture =
        WritesHelper.streamIntoWriteFuture(
            () -> new ByteArrayInputStream(new byte[100]), external, digestOf(100), 1, SECONDS);

    assertThat(writtenFuture.isDone()).isFalse();

    IOException cause = new IOException("server failed");
    writeFuture.setException(cause);

    assertThat(writtenFuture.isDone()).isTrue();
    ExecutionException ee = assertThrows(ExecutionException.class, writtenFuture::get);
    assertThat(ee.getCause()).isSameInstanceAs(cause);
    // cleanup invoked write.cancel on the setException path so the underlying call is torn
    // down explicitly rather than relying on out.close() finding it already broken.
    assertThat(cancelCount.get()).isEqualTo(1);
  }

  @Test
  public void writeFutureFailedExternally_exitsProducerLoopPromptly() throws Exception {
    // Pins the isDone() poll: if write.getFuture() completes exceptionally while the producer
    // is mid-drain on a stream whose isReady() stays true (non-gRPC-like), the next loop
    // iteration must observe the terminal state and exit — not write another chunk.
    byte[] payload = new byte[2 * 128 * 1024 + 7];
    SettableFuture<Long> writeFuture = SettableFuture.create();
    AtomicInteger writeCount = new AtomicInteger();

    Write external =
        new Write() {
          @Override
          public long getCommittedSize() {
            return 0;
          }

          @Override
          public boolean isComplete() {
            return writeFuture.isDone();
          }

          @Override
          public FeedbackOutputStream getOutput(
              long offset, long da, TimeUnit du, Runnable onReadyHandler) {
            return new FeedbackOutputStream() {
              @Override
              public boolean isReady() {
                return true;
              }

              @Override
              public void write(int b) {}

              @Override
              public void write(byte[] b, int off, int len) {
                writeCount.incrementAndGet();
                // Simulate server onError landing between chunks: complete the write
                // future exceptionally after the first chunk lands. The producer's
                // isDone() poll at the top of the next iteration must catch it and exit.
                if (writeCount.get() == 1) {
                  writeFuture.setException(new IOException("server failed mid-drain"));
                }
              }
            };
          }

          @Override
          public ListenableFuture<FeedbackOutputStream> getOutputFuture(
              long offset, long da, TimeUnit du, Runnable r) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void reset() {}

          @Override
          public ListenableFuture<Long> getFuture() {
            return writeFuture;
          }
        };

    ListenableFuture<Long> writtenFuture =
        WritesHelper.streamIntoWriteFuture(
            () -> new ByteArrayInputStream(payload),
            external,
            digestOf(payload.length),
            1,
            SECONDS);

    assertThat(writtenFuture.isDone()).isTrue();
    // Exactly one chunk was written: the producer poll caught the terminal state before
    // chunks #2 and #3 (without the broaden-to-isDone() poll, all three chunks would land).
    assertThat(writeCount.get()).isEqualTo(1);
  }

  @Test
  public void cancellation_beforeEof_closesInputAndOutputViaCleanup() throws Exception {
    SettableFuture<Long> writeFuture = SettableFuture.create();
    AtomicInteger outCloseCount = new AtomicInteger();
    AtomicInteger cancelCount = new AtomicInteger();
    Write parked =
        new Write() {
          @Override
          public long getCommittedSize() {
            return 0;
          }

          @Override
          public boolean isComplete() {
            return writeFuture.isDone();
          }

          @Override
          public FeedbackOutputStream getOutput(
              long offset, long da, TimeUnit du, Runnable onReadyHandler) {
            return new FeedbackOutputStream() {
              @Override
              public boolean isReady() {
                return false;
              }

              @Override
              public void write(int b) {}

              @Override
              public void close() {
                outCloseCount.incrementAndGet();
              }
            };
          }

          @Override
          public ListenableFuture<FeedbackOutputStream> getOutputFuture(
              long offset, long da, TimeUnit du, Runnable r) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void reset() {}

          @Override
          public void cancel(String message, Throwable cause) {
            cancelCount.incrementAndGet();
            writeFuture.setException(Status.CANCELLED.asException());
          }

          @Override
          public ListenableFuture<Long> getFuture() {
            return writeFuture;
          }
        };

    CountingInputStream in = new CountingInputStream(new ByteArrayInputStream(new byte[1024]));
    ListenableFuture<Long> writtenFuture =
        WritesHelper.streamIntoWriteFuture(() -> in, parked, digestOf(1024), 1, SECONDS);

    assertThat(writtenFuture.isDone()).isFalse();

    writtenFuture.cancel(/* mayInterruptIfRunning= */ true);

    // cleanup ran: write.cancel called once, in closed, out closed (since EOF wasn't reached).
    assertThat(cancelCount.get()).isEqualTo(1);
    assertThat(in.closeCount.get()).isEqualTo(1);
    assertThat(outCloseCount.get()).isEqualTo(1);
  }

  @Test
  public void committedSizeMatchesDigestSize_setsFutureWithSize() throws Exception {
    byte[] payload = new byte[64];
    CapturingWrite capturing = new CapturingWrite();
    ListenableFuture<Long> future =
        WritesHelper.streamIntoWriteFuture(
            () -> new ByteArrayInputStream(payload),
            capturing,
            digestOf(payload.length),
            1,
            SECONDS);
    assertThat(future.get()).isEqualTo(payload.length);
  }

  @Test
  public void committedSizeMismatch_logsWarningAndSetsFutureWithDigestSize() throws Exception {
    // Use a Write whose getFuture() completes with a non-matching, non-(-1) value. The helper
    // should still resolve writtenFuture to digest.getSize() (warn-and-trust contract).
    SettableFuture<Long> writeFuture = SettableFuture.create();
    Write mismatched =
        new Write() {
          @Override
          public long getCommittedSize() {
            return 0;
          }

          @Override
          public boolean isComplete() {
            return writeFuture.isDone();
          }

          @Override
          public FeedbackOutputStream getOutput(long offset, long da, TimeUnit du, Runnable r) {
            return new FeedbackOutputStream() {
              @Override
              public boolean isReady() {
                return false;
              }

              @Override
              public void write(int b) {}
            };
          }

          @Override
          public ListenableFuture<FeedbackOutputStream> getOutputFuture(
              long offset, long da, TimeUnit du, Runnable r) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void reset() {}

          @Override
          public ListenableFuture<Long> getFuture() {
            return writeFuture;
          }
        };

    ListenableFuture<Long> writtenFuture =
        WritesHelper.streamIntoWriteFuture(
            () -> new ByteArrayInputStream(new byte[100]), mismatched, digestOf(100), 1, SECONDS);

    // Server returns a nonsensical committedSize (neither 100 nor -1).
    writeFuture.set(42L);

    assertThat(writtenFuture.get()).isEqualTo(100L);
  }

  @Test
  public void supplierOpensFreshStreamPerCall() throws Exception {
    AtomicInteger supplierInvocations = new AtomicInteger();
    byte[] payload = "x".getBytes();

    CapturingWrite c1 = new CapturingWrite();
    CapturingWrite c2 = new CapturingWrite();

    IOSupplier<InputStream> supplier =
        () -> {
          supplierInvocations.incrementAndGet();
          return new ByteArrayInputStream(payload);
        };

    WritesHelper.streamIntoWriteFuture(supplier, c1, digestOf(payload.length), 1, SECONDS).get();
    WritesHelper.streamIntoWriteFuture(supplier, c2, digestOf(payload.length), 1, SECONDS).get();

    assertThat(supplierInvocations.get()).isEqualTo(2);
  }

  @Test
  public void zeroByteBlob_completeWrite_returnsImmediately() throws Exception {
    ListenableFuture<Long> future =
        WritesHelper.streamIntoWriteFuture(
            () -> new ByteArrayInputStream(new byte[0]),
            new CompleteWrite(0),
            digestOf(0),
            1,
            SECONDS);
    assertThat(future.get()).isEqualTo(0);
  }

  @Test
  public void outCloseAtEofFailure_propagatesAndDoesNotDoubleClose() throws Exception {
    // A Write whose close() throws IOException. Helper should propagate it and not retry close
    // in cleanup.
    IOException closeFailure = new IOException("close failed");
    AtomicInteger closeCount = new AtomicInteger();
    Write failingClose =
        new Write() {
          final SettableFuture<Long> future = SettableFuture.create();

          @Override
          public long getCommittedSize() {
            return 0;
          }

          @Override
          public boolean isComplete() {
            return future.isDone();
          }

          @Override
          public FeedbackOutputStream getOutput(long offset, long da, TimeUnit du, Runnable r) {
            return new FeedbackOutputStream() {
              @Override
              public boolean isReady() {
                return true;
              }

              @Override
              public void write(int b) {}

              @Override
              public void write(byte[] b, int off, int len) {}

              @Override
              public void close() throws IOException {
                closeCount.incrementAndGet();
                throw closeFailure;
              }
            };
          }

          @Override
          public ListenableFuture<FeedbackOutputStream> getOutputFuture(
              long offset, long da, TimeUnit du, Runnable r) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void reset() {}

          @Override
          public ListenableFuture<Long> getFuture() {
            return future;
          }
        };

    ListenableFuture<Long> writtenFuture =
        WritesHelper.streamIntoWriteFuture(
            () -> new ByteArrayInputStream(new byte[5]), failingClose, digestOf(5), 1, SECONDS);

    assertThat(writtenFuture.isDone()).isTrue();
    ExecutionException ee = assertThrows(ExecutionException.class, writtenFuture::get);
    assertThat(ee.getCause()).isSameInstanceAs(closeFailure);
    // close() was attempted exactly once — cleanup did NOT retry.
    assertThat(closeCount.get()).isEqualTo(1);
  }

  @Test
  public void getOutputThrowsWriteCompleteException_resolvesViaWriteFutureAndClosesInput()
      throws Exception {
    // TOCTOU: isComplete() returns false at the helper's pre-check, then the underlying write
    // races to completion before getOutput() is called. The contract guarantees getFuture() is
    // complete at that point, so the helper must resolve as success — not propagate the
    // WriteCompleteException as a failure — and must still close the just-opened InputStream.
    long size = 7;
    SettableFuture<Long> writeFuture = SettableFuture.create();
    writeFuture.set(size); // race winner: write is already complete by the time getOutput runs
    AtomicInteger isCompleteCallCount = new AtomicInteger();
    Write raceCompletion =
        new Write() {
          @Override
          public long getCommittedSize() {
            return size;
          }

          @Override
          public boolean isComplete() {
            // False on the helper's pre-check, true on subsequent calls. Mirrors the race where
            // the write completes between isComplete() and getOutput().
            return isCompleteCallCount.getAndIncrement() > 0;
          }

          @Override
          public FeedbackOutputStream getOutput(
              long offset, long da, TimeUnit du, Runnable onReadyHandler)
              throws WriteCompleteException {
            throw new WriteCompleteException();
          }

          @Override
          public ListenableFuture<FeedbackOutputStream> getOutputFuture(
              long offset, long da, TimeUnit du, Runnable r) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void reset() {}

          @Override
          public ListenableFuture<Long> getFuture() {
            return writeFuture;
          }
        };

    CountingInputStream in =
        new CountingInputStream(new ByteArrayInputStream(new byte[(int) size]));
    ListenableFuture<Long> writtenFuture =
        WritesHelper.streamIntoWriteFuture(() -> in, raceCompletion, digestOf(size), 1, SECONDS);

    assertThat(writtenFuture.isDone()).isTrue();
    assertThat(writtenFuture.get()).isEqualTo(size);
    // The just-opened InputStream is closed by the WriteCompleteException catch branch.
    assertThat(in.closeCount.get()).isEqualTo(1);
  }

  @Test
  public void getOutputThrowsNonWriteCompleteIoException_propagatesAndClosesInput() {
    // Other IOExceptions from getOutput must propagate synchronously without leaking the
    // just-opened InputStream — attachListeners() hasn't run yet, so cleanup() is not wired
    // to release it.
    IOException getOutputFailure = new IOException("getOutput failed");
    Write failing =
        new Write() {
          @Override
          public long getCommittedSize() {
            return 0;
          }

          @Override
          public boolean isComplete() {
            return false;
          }

          @Override
          public FeedbackOutputStream getOutput(
              long offset, long da, TimeUnit du, Runnable onReadyHandler) throws IOException {
            throw getOutputFailure;
          }

          @Override
          public ListenableFuture<FeedbackOutputStream> getOutputFuture(
              long offset, long da, TimeUnit du, Runnable r) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void reset() {}

          @Override
          public ListenableFuture<Long> getFuture() {
            return SettableFuture.create();
          }
        };

    CountingInputStream in = new CountingInputStream(new ByteArrayInputStream(new byte[10]));
    ListenableFuture<Long> future =
        WritesHelper.streamIntoWriteFuture(() -> in, failing, digestOf(10), 1, SECONDS);
    assertThat(future.isDone()).isTrue();
    ExecutionException thrown = assertThrows(ExecutionException.class, future::get);
    assertThat(thrown).hasCauseThat().isSameInstanceAs(getOutputFailure);
    assertThat(in.closeCount.get()).isEqualTo(1);
  }
}
