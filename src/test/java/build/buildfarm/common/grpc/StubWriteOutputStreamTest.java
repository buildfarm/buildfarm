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

package build.buildfarm.common.grpc;

import static com.google.common.truth.Truth.assertThat;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamGrpc.ByteStreamImplBase;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusRequest;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusResponse;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.common.base.Suppliers;
import com.google.protobuf.ByteString;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.grpc.util.MutableHandlerRegistry;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class StubWriteOutputStreamTest {
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private final MutableHandlerRegistry serviceRegistry = new MutableHandlerRegistry();

  private Channel channel;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    String serverName = InProcessServerBuilder.generateName();

    grpcCleanup
        .register(
            InProcessServerBuilder.forName(serverName)
                .fallbackHandlerRegistry(serviceRegistry)
                .directExecutor()
                .build())
        .start();

    channel =
        grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build());
  }

  /** Counts {@code ClientCall.cancel(...)} invocations on the test channel. */
  private static ClientInterceptor recordingCancelInterceptor(AtomicInteger cancelCount) {
    return new ClientInterceptor() {
      @Override
      public <Q, R> ClientCall<Q, R> interceptCall(
          MethodDescriptor<Q, R> method, CallOptions callOptions, Channel next) {
        return new SimpleForwardingClientCall<Q, R>(next.newCall(method, callOptions)) {
          @Override
          public void cancel(String message, Throwable cause) {
            cancelCount.incrementAndGet();
            super.cancel(message, cause);
          }
        };
      }
    };
  }

  /** Registers a ByteStream service whose {@code write} returns a fresh mock request observer. */
  @SuppressWarnings("unchecked")
  private void registerMockByteStreamService() {
    serviceRegistry.addService(
        new ByteStreamImplBase() {
          @Override
          public StreamObserver<WriteRequest> write(
              StreamObserver<WriteResponse> responseObserver) {
            return mock(StreamObserver.class);
          }
        });
  }

  /**
   * Registers a ByteStream service that captures the response observer into the given reference and
   * returns a fresh mock request observer. Lets tests drive the response side via {@code
   * captured.get().onNext / onError / onCompleted}.
   */
  @SuppressWarnings("unchecked")
  private void registerCapturingByteStreamService(
      AtomicReference<StreamObserver<WriteResponse>> captured) {
    serviceRegistry.addService(
        new ByteStreamImplBase() {
          @Override
          public StreamObserver<WriteRequest> write(
              StreamObserver<WriteResponse> responseObserver) {
            captured.set(responseObserver);
            return mock(StreamObserver.class);
          }
        });
  }

  private StubWriteOutputStream newWrite(Channel ch, String resourceName, long expectedSize) {
    return new StubWriteOutputStream(
        /* bsBlockingStub= */ null,
        Suppliers.ofInstance(ByteStreamGrpc.newStub(ch)),
        resourceName,
        e -> e,
        expectedSize,
        /* autoflush= */ false);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void resetExceptionsAreInterpreted() {
    String unimplementedResourceName = "unimplemented-resource";
    String notFoundResourceName = "not-found-resource";
    serviceRegistry.addService(
        new ByteStreamImplBase() {
          @Override
          public void queryWriteStatus(
              QueryWriteStatusRequest request,
              StreamObserver<QueryWriteStatusResponse> responseObserver) {
            if (request.getResourceName().equals(unimplementedResourceName)) {
              responseObserver.onError(Status.UNIMPLEMENTED.asException());
            } else if (request.getResourceName().equals(notFoundResourceName)) {
              responseObserver.onError(Status.NOT_FOUND.asException());
            } else {
              responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            }
          }
        });

    StubWriteOutputStream write =
        new StubWriteOutputStream(
            Suppliers.ofInstance(ByteStreamGrpc.newBlockingStub(channel)),
            Suppliers.ofInstance(ByteStreamGrpc.newStub(channel)),
            unimplementedResourceName,
            e -> e,
            /* expectedSize= */ StubWriteOutputStream.UNLIMITED_EXPECTED_SIZE,
            /* autoflush= */ true);
    assertThat(write.getCommittedSize()).isEqualTo(0);

    write =
        new StubWriteOutputStream(
            Suppliers.ofInstance(ByteStreamGrpc.newBlockingStub(channel)),
            Suppliers.ofInstance(ByteStreamGrpc.newStub(channel)),
            notFoundResourceName,
            e -> e,
            /* expectedSize= */ StubWriteOutputStream.UNLIMITED_EXPECTED_SIZE,
            /* autoflush= */ true);
    assertThat(write.getCommittedSize()).isEqualTo(0);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void resetIsRespectedOnSubsequentWrite() throws IOException {
    StreamObserver<WriteRequest> writeObserver = mock(StreamObserver.class);
    serviceRegistry.addService(
        new ByteStreamImplBase() {
          @Override
          public StreamObserver<WriteRequest> write(
              StreamObserver<WriteResponse> responseObserver) {
            return writeObserver;
          }
        });
    String resourceName = "reset-resource";
    StubWriteOutputStream write =
        new StubWriteOutputStream(
            /* bsBlockingStub= */ null,
            Suppliers.ofInstance(ByteStreamGrpc.newStub(channel)),
            resourceName,
            e -> e,
            /* expectedSize= */ StubWriteOutputStream.UNLIMITED_EXPECTED_SIZE,
            /* autoflush= */ true);
    ByteString content = ByteString.copyFromUtf8("Hello, World");
    try (OutputStream out = write.getOutput(1, SECONDS, () -> {})) {
      content.writeTo(out);
    }

    // implicit reset with getOutput default offset of 0
    try (OutputStream out = write.getOutput(1, SECONDS, () -> {})) {
      content.writeTo(out);
    }
    ArgumentCaptor<WriteRequest> writeRequestCaptor = ArgumentCaptor.forClass(WriteRequest.class);
    verify(writeObserver, times(4)).onNext(writeRequestCaptor.capture());
    List<WriteRequest> requests = writeRequestCaptor.getAllValues();
    // request 0 - write at 0
    // request 1 - finishWrite for close
    // request 2 - write complete at 0
    // request 3 - finishWrite for close
    assertThat(requests.get(1).getFinishWrite()).isTrue();
    assertThat(requests.get(0).getWriteOffset()).isEqualTo(requests.get(2).getWriteOffset());
    assertThat(requests.get(3).getFinishWrite()).isTrue();
  }

  @Test
  public void getOutputOffsetMatchesCommittedSize() throws IOException {
    StreamObserver<WriteRequest> writeObserver = mock(StreamObserver.class);
    serviceRegistry.addService(
        new ByteStreamImplBase() {
          @Override
          public void queryWriteStatus(
              QueryWriteStatusRequest request,
              StreamObserver<QueryWriteStatusResponse> responseObserver) {
            responseObserver.onNext(
                QueryWriteStatusResponse.newBuilder().setCommittedSize(20).build());
            responseObserver.onCompleted();
          }

          @Override
          public StreamObserver<WriteRequest> write(
              StreamObserver<WriteResponse> responseObserver) {
            return writeObserver;
          }
        });
    String resourceName = "resumed-resource";
    StubWriteOutputStream write =
        new StubWriteOutputStream(
            Suppliers.ofInstance(ByteStreamGrpc.newBlockingStub(channel)),
            Suppliers.ofInstance(ByteStreamGrpc.newStub(channel)),
            resourceName,
            e -> e,
            /* expectedSize= */ 40,
            /* autoflush= */ false);

    write.getOutput(20, 1, SECONDS, () -> {});
    assertThat(write.getCommittedSize()).isEqualTo(20);
    verifyNoInteractions(writeObserver);
  }

  @Test
  public void serverErrorMakesCloseANoOp() throws IOException {
    // After the response observer's onError, the gRPC stream is fully closed by the server and
    // Netty's onStreamClosed drains pendingWriteQueue. onError nulls writeObserver and marks the
    // stream fully torn down, so a subsequent close() is a no-op and does not re-initiate cancel.
    AtomicInteger cancelCount = new AtomicInteger();
    AtomicReference<StreamObserver<WriteResponse>> capturedResponseObserver =
        new AtomicReference<>();
    registerCapturingByteStreamService(capturedResponseObserver);

    StubWriteOutputStream write =
        newWrite(
            ClientInterceptors.intercept(channel, recordingCancelInterceptor(cancelCount)),
            "sad-path-resource",
            /* expectedSize= */ 100);

    OutputStream outputStream = write.getOutput(1, SECONDS, () -> {});
    assertThat(write.isGrpcRemoteCallClosed()).isFalse();

    capturedResponseObserver.get().onError(Status.UNAVAILABLE.asException());

    assertThat(write.isGrpcRemoteCallClosed()).isTrue();

    // close() is a no-op — it does not re-throw the prior failure as
    // UncheckedExecutionException (which WriteStreamObserver's cancellation listener would log
    // SEVERE) and does not initiate cancel (the stream is CLOSED, not HALF_CLOSED_REMOTE, so
    // Netty's onStreamClosed already handled release).
    outputStream.close();
    assertThat(cancelCount.get()).isEqualTo(0);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void remoteStreamClosingFirstDoesNotPreventLocalStreamFromClosing() throws IOException {
    // Observe the client call: the in-process server stops receiving signals once it closes,
    // so its request observer cannot tell us whether the client subsequently half-closes.
    ClientCall<WriteRequest, WriteResponse> call = mock(ClientCall.class);
    Channel mockChannel = mock(Channel.class);
    when(mockChannel.<WriteRequest, WriteResponse>newCall(any(), any())).thenReturn(call);
    StubWriteOutputStream write = newWrite(mockChannel, "remote-close-resource", 100);
    OutputStream out = write.getOutput(1, SECONDS, () -> {});
    ArgumentCaptor<ClientCall.Listener<WriteResponse>> listener =
        ArgumentCaptor.forClass(ClientCall.Listener.class);
    verify(call).start(listener.capture(), any(Metadata.class));

    listener.getValue().onMessage(WriteResponse.newBuilder().setCommittedSize(100).build());
    listener.getValue().onClose(Status.OK, new Metadata());
    verify(call, never()).halfClose();

    out.close();
    out.close();

    verify(call).halfClose();
    verify(call, never()).cancel(any(), any());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void closeHappyPathSendsOnCompleted() throws IOException {
    StreamObserver<WriteRequest> remoteStreamObserver = mock(StreamObserver.class);
    serviceRegistry.addService(
        new ByteStreamImplBase() {
          @Override
          public StreamObserver<WriteRequest> write(
              StreamObserver<WriteResponse> responseObserver) {
            return remoteStreamObserver;
          }
        });

    String resourceName = "happy-path-resource";
    StubWriteOutputStream write =
        new StubWriteOutputStream(
            /* bsBlockingStub= */ null,
            Suppliers.ofInstance(ByteStreamGrpc.newStub(channel)),
            resourceName,
            e -> e,
            /* expectedSize= */ StubWriteOutputStream.UNLIMITED_EXPECTED_SIZE,
            /* autoflush= */ true);

    ByteString content = ByteString.copyFromUtf8("Hello, World");
    try (OutputStream out = write.getOutput(1, SECONDS, () -> {})) {
      content.writeTo(out);
    }

    // Two onNext calls:
    // 1. write at 0
    // 2. finishWrite for close
    // Then exactly one onCompleted
    verify(remoteStreamObserver, times(2)).onNext(any(WriteRequest.class));
    verify(remoteStreamObserver, times(1)).onCompleted();
    verifyNoMoreInteractions(remoteStreamObserver);
  }

  @Test
  public void closeWhenWriteFutureFailedFromOnError_isQuiet() throws IOException {
    // close() must not re-throw the prior failure: WriteStreamObserver's cancellation listener
    // catches only IOException, so a RuntimeException would log SEVERE on every cancellation.
    AtomicReference<StreamObserver<WriteResponse>> capturedResponseObserver =
        new AtomicReference<>();
    registerCapturingByteStreamService(capturedResponseObserver);

    StubWriteOutputStream write =
        newWrite(channel, "error-then-close-resource", /* expectedSize= */ 100);

    OutputStream out = write.getOutput(1, SECONDS, () -> {});
    capturedResponseObserver.get().onError(Status.UNAVAILABLE.asException());

    // close() must not throw RuntimeException (UncheckedExecutionException of the prior cause).
    out.close();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void writeAfterServerError_surfacesPriorFailureAndDoesNotInitiateNewCall()
      throws IOException {
    // After onError, the response observer has nulled writeObserver. A follow-up write() must
    // surface the prior failure via checkComplete() rather than fall through to initiateWrite()
    // and open a fresh gRPC call (which would leak — close() short-circuits on isFullyClosed).
    // We verify both: the failure throws, AND the underlying ByteStream.write was called exactly
    // once (the initial getOutput) — no leaked re-initiation.
    AtomicInteger writeCallCount = new AtomicInteger();
    AtomicReference<StreamObserver<WriteResponse>> capturedResponseObserver =
        new AtomicReference<>();
    serviceRegistry.addService(
        new ByteStreamImplBase() {
          @Override
          public StreamObserver<WriteRequest> write(
              StreamObserver<WriteResponse> responseObserver) {
            writeCallCount.incrementAndGet();
            capturedResponseObserver.set(responseObserver);
            return mock(StreamObserver.class);
          }
        });

    StubWriteOutputStream write =
        newWrite(channel, "errored-write-resource", /* expectedSize= */ 100);
    write.getOutput(1, SECONDS, () -> {});
    assertThat(writeCallCount.get()).isEqualTo(1);

    capturedResponseObserver.get().onError(Status.UNAVAILABLE.asException());

    // A follow-up write surfaces the prior failure. gRPC wraps the server's StatusException as
    // StatusRuntimeException on the client, which checkComplete throwIfUnchecked()s directly — we
    // assert on the underlying status code rather than the concrete wrapper class.
    RuntimeException byteArrayEx =
        assertThrows(RuntimeException.class, () -> write.write(new byte[] {1}, 0, 1));
    assertThat(Status.fromThrowable(byteArrayEx).getCode()).isEqualTo(Status.Code.UNAVAILABLE);

    // Crucially: no new gRPC call was initiated. The original observer was already nulled by
    // onError; absent the checkComplete guard, write(...) would have realized a fresh
    // writeObserver.
    assertThat(writeCallCount.get()).isEqualTo(1);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void getOutput_afterServerError_throwsIOExceptionAndDoesNotOpenNewCall()
      throws IOException {
    // Symmetric to the cancel case for the response observer's onError teardown path. onError
    // sets isFullyClosed=true, so a follow-up getOutput must not open a fresh call.
    AtomicInteger writeCallCount = new AtomicInteger();
    AtomicReference<StreamObserver<WriteResponse>> capturedResponseObserver =
        new AtomicReference<>();
    serviceRegistry.addService(
        new ByteStreamImplBase() {
          @Override
          public StreamObserver<WriteRequest> write(
              StreamObserver<WriteResponse> responseObserver) {
            writeCallCount.incrementAndGet();
            capturedResponseObserver.set(responseObserver);
            return mock(StreamObserver.class);
          }
        });

    StubWriteOutputStream write =
        newWrite(channel, "error-then-getoutput-resource", /* expectedSize= */ 100);
    write.getOutput(1, SECONDS, () -> {});
    assertThat(writeCallCount.get()).isEqualTo(1);

    capturedResponseObserver.get().onError(Status.UNAVAILABLE.asException());
    assertThat(write.isFullyClosed()).isTrue();

    IOException ex = assertThrows(IOException.class, () -> write.getOutput(1, SECONDS, () -> {}));
    // Server's UNAVAILABLE is preserved as the cause (gRPC client wraps it as
    // StatusRuntimeException; Status.fromThrowable walks the chain).
    assertThat(Status.fromThrowable(ex).getCode()).isEqualTo(Status.Code.UNAVAILABLE);

    assertThat(writeCallCount.get()).isEqualTo(1);
  }

  @Test
  public void getOutput_afterServerErrorWhileInterrupted_preservesInterruptedException()
      throws IOException {
    AtomicReference<StreamObserver<WriteResponse>> capturedResponseObserver =
        new AtomicReference<>();
    registerCapturingByteStreamService(capturedResponseObserver);

    StubWriteOutputStream write =
        newWrite(channel, "interrupted-after-error-resource", /* expectedSize= */ 100);
    write.getOutput(1, SECONDS, () -> {});
    capturedResponseObserver.get().onError(Status.UNAVAILABLE.asException());

    Thread.currentThread().interrupt();
    try {
      IOException exception =
          assertThrows(IOException.class, () -> write.getOutput(1, SECONDS, () -> {}));

      assertThat(exception).hasCauseThat().isInstanceOf(InterruptedException.class);
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
    } finally {
      // Do not leak the interrupt into the test runner.
      Thread.interrupted();
    }
  }
}
