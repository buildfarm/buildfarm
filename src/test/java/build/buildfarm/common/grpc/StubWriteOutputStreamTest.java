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
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamGrpc.ByteStreamImplBase;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusRequest;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusResponse;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.common.base.Suppliers;
import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.grpc.util.MutableHandlerRegistry;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.ExecutionException;
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

  @SuppressWarnings("PMD.EmptyControlStatement")
  @Test
  public void getOutputCallback() throws Exception {
    String resourceName = "reset-resource";
    StubWriteOutputStream write =
        new StubWriteOutputStream(
            /* bsBlockingStub= */ null,
            Suppliers.ofInstance(ByteStreamGrpc.newStub(channel)),
            resourceName,
            e -> e,
            /* expectedSize= */ StubWriteOutputStream.UNLIMITED_EXPECTED_SIZE,
            /* autoflush= */ true);

    boolean callbackTimedOut = false;
    try (OutputStream ignored =
        write.getOutput(
            1,
            MICROSECONDS,
            () -> {
              try {
                Thread.sleep(1000);
              } catch (InterruptedException e) {
              }
            })) {
      write.getFuture().get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof StatusRuntimeException sre) {
        callbackTimedOut = Status.fromThrowable(sre).getCode() == Status.Code.DEADLINE_EXCEEDED;
      }
    }
    assertThat(callbackTimedOut).isTrue();
  }

  @Test
  public void getOutputOffsetMatchesCommittedSize() {
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
}
