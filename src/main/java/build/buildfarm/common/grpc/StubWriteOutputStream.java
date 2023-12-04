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

package build.buildfarm.common.grpc;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.lang.String.format;
import static java.util.logging.Level.WARNING;

import build.buildfarm.common.Write;
import build.buildfarm.common.io.FeedbackOutputStream;
import com.google.bytestream.ByteStreamGrpc.ByteStreamBlockingStub;
import com.google.bytestream.ByteStreamGrpc.ByteStreamStub;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusRequest;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusResponse;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.annotation.concurrent.GuardedBy;
import lombok.extern.java.Log;

@Log
public class StubWriteOutputStream extends FeedbackOutputStream implements Write {
  public static final long UNLIMITED_EXPECTED_SIZE = Long.MAX_VALUE;

  private static final int CHUNK_SIZE = 16 * 1024;

  private static final QueryWriteStatusResponse resetResponse =
      QueryWriteStatusResponse.newBuilder().setCommittedSize(0).setComplete(false).build();

  @SuppressWarnings("Guava")
  private final Supplier<ByteStreamBlockingStub> bsBlockingStub;

  @SuppressWarnings("Guava")
  private final Supplier<ByteStreamStub> bsStub;

  private final String resourceName;
  private final Function<Throwable, Throwable> exceptionTranslator;
  private final long expectedSize;
  private final boolean autoflush;
  private final byte[] buf;
  private final SettableFuture<Long> writeFuture = SettableFuture.create();
  private boolean wasReset = false;

  @SuppressWarnings({"unchecked", "rawtypes", "Guava"})
  private final Supplier<QueryWriteStatusResponse> writeStatus =
      Suppliers.memoize(
          new Supplier() {
            @Override
            public QueryWriteStatusResponse get() {
              if (wasReset) {
                return resetResponse;
              }
              try {
                QueryWriteStatusResponse response =
                    bsBlockingStub
                        .get()
                        .queryWriteStatus(
                            QueryWriteStatusRequest.newBuilder()
                                .setResourceName(resourceName)
                                .build());
                if (response.getComplete()) {
                  writeFuture.set(response.getCommittedSize());
                }
                return response;
              } catch (StatusRuntimeException e) {
                Status status = Status.fromThrowable(e);
                if (status.getCode() == Code.UNIMPLEMENTED || status.getCode() == Code.NOT_FOUND) {
                  return resetResponse;
                }
                throw e;
              }
            }
          });

  private boolean sentResourceName = false;
  private int offset = 0;
  private long writtenBytes = 0;

  @GuardedBy("this")
  private StreamObserver<WriteRequest> writeObserver = null;

  private long deadlineAfter = 0;
  private TimeUnit deadlineAfterUnits = null;
  private Runnable onReadyHandler = null;

  @SuppressWarnings("Guava")
  public StubWriteOutputStream(
      Supplier<ByteStreamBlockingStub> bsBlockingStub,
      Supplier<ByteStreamStub> bsStub,
      String resourceName,
      Function<Throwable, Throwable> exceptionTranslator,
      long expectedSize,
      boolean autoflush) {
    this.bsBlockingStub = bsBlockingStub;
    this.bsStub = bsStub;
    this.exceptionTranslator = exceptionTranslator;
    this.resourceName = resourceName;
    this.expectedSize = expectedSize;
    this.autoflush = autoflush;
    buf = new byte[(int) Math.min(CHUNK_SIZE, expectedSize)];
  }

  @Override
  public void close() throws IOException {
    if (!checkComplete()) {
      boolean finishWrite = expectedSize == UNLIMITED_EXPECTED_SIZE;
      if (finishWrite || offset != 0) {
        initiateWrite();
        flushSome(finishWrite);
      }
      synchronized (this) {
        if (writeObserver != null) {
          if (finishWrite || getCommittedSize() + offset == expectedSize) {
            writeObserver.onCompleted();
          } else {
            writeObserver.onError(Status.CANCELLED.asException());
          }
          writeObserver = null;
        }
      }
    }
  }

  private void flushSome(boolean finishWrite) {
    WriteRequest.Builder request =
        WriteRequest.newBuilder()
            .setWriteOffset(getCommittedSize())
            .setData(ByteString.copyFrom(buf, 0, offset))
            .setFinishWrite(finishWrite);
    if (!sentResourceName) {
      request.setResourceName(resourceName);
    }
    synchronized (this) {
      writeObserver.onNext(request.build());
    }
    wasReset = false;
    writtenBytes += offset;
    offset = 0;
    sentResourceName = true;
  }

  @Override
  public void flush() throws IOException {
    if (!checkComplete() && offset != 0) {
      initiateWrite();
      flushSome(getCommittedSize() + offset == expectedSize);
    }
  }

  private boolean checkComplete() throws IOException {
    try {
      return writeFuture.isDone() && writeFuture.get() >= 0;
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      Throwables.throwIfUnchecked(cause);
      Throwables.throwIfInstanceOf(cause, IOException.class);
      throw new UncheckedExecutionException(cause);
    } catch (InterruptedException e) {
      // unlikely, since we only get for isDone()
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private synchronized void initiateWrite() {
    if (writeObserver == null) {
      checkNotNull(deadlineAfterUnits);
      writeObserver =
          bsStub
              .get()
              .withDeadlineAfter(deadlineAfter, deadlineAfterUnits)
              .write(
                  new ClientResponseObserver<WriteRequest, WriteResponse>() {
                    @Override
                    public void beforeStart(ClientCallStreamObserver<WriteRequest> requestStream) {
                      requestStream.setOnReadyHandler(
                          () -> {
                            if (requestStream.isReady()) {
                              onReadyHandler.run();
                            }
                          });
                    }

                    @Override
                    public void onNext(WriteResponse response) {
                      long committedSize = response.getCommittedSize();
                      if (expectedSize != UNLIMITED_EXPECTED_SIZE
                          && committedSize != expectedSize) {
                        log.log(
                            WARNING,
                            format(
                                "received WriteResponse with unexpected committedSize: %d != %d",
                                committedSize, expectedSize));
                        committedSize = expectedSize;
                      }
                      writeFuture.set(committedSize);
                    }

                    @Override
                    public void onError(Throwable t) {
                      log.log(
                          WARNING,
                          format(
                              "%s: write(%s) on worker %s after %d bytes of content",
                              Status.fromThrowable(t).getCode().name(),
                              resourceName,
                              bsStub.get().getChannel().authority(),
                              writtenBytes));
                      writeFuture.setException(exceptionTranslator.apply(t));
                    }

                    @Override
                    public void onCompleted() {
                      synchronized (StubWriteOutputStream.this) {
                        writeObserver = null;
                      }
                    }
                  });
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (isComplete()) {
      throw new WriteCompleteException();
    }
    if (getCommittedSize() + offset + len > expectedSize) {
      throw new IndexOutOfBoundsException("write would exceed expected size");
    }
    boolean lastFlushed = false;
    while (len > 0 && !checkComplete()) {
      lastFlushed = false;
      int copyLen = Math.min(buf.length - offset, len);
      System.arraycopy(b, off, buf, offset, copyLen);
      offset += copyLen;
      off += copyLen;
      len -= copyLen;
      if (offset == buf.length || getCommittedSize() + offset == expectedSize) {
        flush();
        lastFlushed = true;
      }
    }
    if (!lastFlushed && autoflush) {
      flush();
    }
  }

  @Override
  public void write(int b) throws IOException {
    if (isComplete()) {
      throw new WriteCompleteException();
    }
    if (!checkComplete()) {
      buf[offset++] = (byte) b;
      if (autoflush || offset == buf.length) {
        flush();
      }
    }
  }

  @Override
  public synchronized boolean isReady() {
    checkNotNull(writeObserver);
    ClientCallStreamObserver<WriteRequest> clientCallStreamObserver =
        (ClientCallStreamObserver<WriteRequest>) writeObserver;
    return clientCallStreamObserver.isReady();
  }

  // Write methods

  @Override
  public long getCommittedSize() {
    try {
      if (checkComplete()) {
        try {
          return writeFuture.get();
        } catch (InterruptedException e) {
          // impossible, future must be done
          throw new RuntimeException(e);
        } catch (ExecutionException e) {
          // impossible, future throws in checkComplete for this
          throw new UncheckedExecutionException(e.getCause());
        }
      }
      return writeStatus.get().getCommittedSize() + writtenBytes;
    } catch (IOException e) {
      // errored write does not have any progress
      return 0;
    }
  }

  @Override
  public boolean isComplete() {
    try {
      return checkComplete() || getCommittedSize() == expectedSize;
    } catch (IOException e) {
      // errored write is not complete
      return false;
    }
  }

  @Override
  public FeedbackOutputStream getOutput(
      long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler) {
    this.deadlineAfter = deadlineAfter;
    this.deadlineAfterUnits = deadlineAfterUnits;
    this.onReadyHandler = onReadyHandler;
    synchronized (this) {
      if (writeObserver == null) {
        initiateWrite();
      }
    }
    return this;
  }

  @Override
  public ListenableFuture<FeedbackOutputStream> getOutputFuture(
      long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler) {
    return immediateFuture(getOutput(deadlineAfter, deadlineAfterUnits, onReadyHandler));
  }

  @Override
  public void reset() {
    if (!writeFuture.isDone()) {
      wasReset = true;
      offset = 0;
      writtenBytes = 0;
    }
  }

  @Override
  public ListenableFuture<Long> getFuture() {
    return writeFuture;
  }
}
