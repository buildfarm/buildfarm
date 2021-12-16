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

import static com.google.common.truth.Truth.assertThat;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamGrpc.ByteStreamImplBase;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusRequest;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusResponse;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.common.base.Functions;
import com.google.common.base.Suppliers;
import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

@RunWith(JUnit4.class)
public class StubWriteOutputStreamTest {
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  @SuppressWarnings("unchecked")
  private final StreamObserver<WriteRequest> writeObserver = mock(StreamObserver.class);

  private final ByteStreamImplBase serviceImpl =
      mock(
          ByteStreamImplBase.class,
          delegatesTo(
              new ByteStreamImplBase() {
                @Override
                public StreamObserver<WriteRequest> write(
                    StreamObserver<WriteResponse> responseObserver) {
                  return writeObserver;
                }
              }));

  private Channel channel;

  @Before
  public void setUp() throws Exception {
    String serverName = InProcessServerBuilder.generateName();

    grpcCleanup
        .register(
            InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(serviceImpl)
                .build())
        .start();

    channel =
        grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void resetExceptionsAreInterpreted() {
    String unimplementedResourceName = "unimplemented-resource";
    QueryWriteStatusRequest unimplementedRequest =
        QueryWriteStatusRequest.newBuilder().setResourceName(unimplementedResourceName).build();
    doAnswer(
            invocation -> {
              StreamObserver<QueryWriteStatusResponse> observer = invocation.getArgument(1);
              observer.onError(Status.UNIMPLEMENTED.asException());
              return null;
            })
        .when(serviceImpl)
        .queryWriteStatus(eq(unimplementedRequest), any(StreamObserver.class));

    String notFoundResourceName = "not-found-resource";
    QueryWriteStatusRequest notFoundRequest =
        QueryWriteStatusRequest.newBuilder().setResourceName(notFoundResourceName).build();
    doAnswer(
            invocation -> {
              StreamObserver<QueryWriteStatusResponse> observer = invocation.getArgument(1);
              observer.onError(Status.NOT_FOUND.asException());
              return null;
            })
        .when(serviceImpl)
        .queryWriteStatus(eq(notFoundRequest), any(StreamObserver.class));

    StubWriteOutputStream write =
        new StubWriteOutputStream(
            Suppliers.ofInstance(ByteStreamGrpc.newBlockingStub(channel)),
            Suppliers.ofInstance(ByteStreamGrpc.newStub(channel)),
            unimplementedResourceName,
            Functions.identity(),
            /* expectedSize=*/ StubWriteOutputStream.UNLIMITED_EXPECTED_SIZE,
            /* autoflush=*/ true);
    assertThat(write.getCommittedSize()).isEqualTo(0);
    verify(serviceImpl, times(1))
        .queryWriteStatus(eq(unimplementedRequest), any(StreamObserver.class));

    write =
        new StubWriteOutputStream(
            Suppliers.ofInstance(ByteStreamGrpc.newBlockingStub(channel)),
            Suppliers.ofInstance(ByteStreamGrpc.newStub(channel)),
            notFoundResourceName,
            Functions.identity(),
            /* expectedSize=*/ StubWriteOutputStream.UNLIMITED_EXPECTED_SIZE,
            /* autoflush=*/ true);
    assertThat(write.getCommittedSize()).isEqualTo(0);
    verify(serviceImpl, times(1)).queryWriteStatus(eq(notFoundRequest), any(StreamObserver.class));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void resetIsRespectedOnSubsequentWrite() throws IOException {
    String resourceName = "reset-resource";
    StubWriteOutputStream write =
        new StubWriteOutputStream(
            Suppliers.ofInstance(ByteStreamGrpc.newBlockingStub(channel)),
            Suppliers.ofInstance(ByteStreamGrpc.newStub(channel)),
            resourceName,
            Functions.identity(),
            /* expectedSize=*/ StubWriteOutputStream.UNLIMITED_EXPECTED_SIZE,
            /* autoflush=*/ true);
    ByteString content = ByteString.copyFromUtf8("Hello, World");
    try (OutputStream out = write.getOutput(1, SECONDS, () -> {})) {
      content.writeTo(out);
      write.reset();
      content.writeTo(out);
    }
    verify(serviceImpl, times(1)).write(any(StreamObserver.class));
    ArgumentCaptor<WriteRequest> writeRequestCaptor = ArgumentCaptor.forClass(WriteRequest.class);
    verify(writeObserver, times(3)).onNext(writeRequestCaptor.capture());
    List<WriteRequest> requests = writeRequestCaptor.getAllValues();
    assertThat(requests.get(0).getWriteOffset()).isEqualTo(requests.get(1).getWriteOffset());
    assertThat(requests.get(2).getFinishWrite()).isTrue();
  }

  @Test
  public void getOutputCallback() throws IOException {
    String resourceName = "reset-resource";
    StubWriteOutputStream write =
        new StubWriteOutputStream(
            Suppliers.ofInstance(ByteStreamGrpc.newBlockingStub(channel)),
            Suppliers.ofInstance(ByteStreamGrpc.newStub(channel)),
            resourceName,
            Functions.identity(),
            /* expectedSize=*/ StubWriteOutputStream.UNLIMITED_EXPECTED_SIZE,
            /* autoflush=*/ true);

    boolean callbackTimedOut = false;
    try (OutputStream out =
        write.getOutput(
            1,
            MICROSECONDS,
            () -> {
              try {
                Thread.sleep(1000);
              } catch (InterruptedException e) {
              }
            })) {
    } catch (Exception e) {
      callbackTimedOut = true;
    }
    assertThat(callbackTimedOut).isTrue();
  }
}
