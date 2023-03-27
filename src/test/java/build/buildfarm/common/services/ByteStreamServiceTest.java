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

package build.buildfarm.common.services;

import static build.buildfarm.common.DigestUtil.HashFunction.SHA256;
import static build.buildfarm.common.services.ByteStreamService.CHUNK_SIZE;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.AdditionalAnswers.answerVoid;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.Write;
import build.buildfarm.common.io.FeedbackOutputStream;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.ByteStreamUploader;
import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamGrpc.ByteStreamStub;
import com.google.bytestream.ByteStreamProto.ReadRequest;
import com.google.bytestream.ByteStreamProto.ReadResponse;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

@RunWith(JUnit4.class)
public class ByteStreamServiceTest {
  private static final DigestUtil DIGEST_UTIL = new DigestUtil(SHA256);

  private Server fakeServer;
  private String fakeServerName;

  @Mock private Instance instance;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    fakeServerName = "fake server for " + getClass();
    // Use a mutable service registry for later registering the service impl for each test case.
    fakeServer =
        InProcessServerBuilder.forName(fakeServerName)
            .addService(new ByteStreamService(instance))
            .directExecutor()
            .build()
            .start();
  }

  @After
  public void tearDown() throws Exception {
    fakeServer.shutdownNow();
    fakeServer.awaitTermination();
  }

  @Test
  public void uploadsCanResetInLine() throws Exception {
    ByteString content = ByteString.copyFromUtf8("Hello, World!");
    Digest digest = DIGEST_UTIL.compute(content);
    UUID uuid = UUID.randomUUID();

    SettableFuture<Long> writtenFuture = SettableFuture.create();
    ByteString.Output output = ByteString.newOutput((int) digest.getSizeBytes());
    FeedbackOutputStream out =
        new FeedbackOutputStream() {
          @Override
          public void close() {
            if (output.size() == digest.getSizeBytes()) {
              writtenFuture.set(digest.getSizeBytes());
            }
          }

          @Override
          public void flush() throws IOException {
            output.flush();
          }

          @Override
          public void write(byte[] b) throws IOException {
            output.write(b);
          }

          @Override
          public void write(byte[] b, int off, int len) throws IOException {
            output.write(b, off, len);
          }

          @Override
          public void write(int b) throws IOException {
            output.write(b);
          }

          @Override
          public boolean isReady() {
            return true;
          }
        };

    Write write = mock(Write.class);
    doAnswer(
            (Answer<Void>)
                invocation -> {
                  output.reset();
                  return null;
                })
        .when(write)
        .reset();
    when(write.getOutput(any(Long.class), any(TimeUnit.class), any(Runnable.class)))
        .thenReturn(out);
    doAnswer(invocation -> (long) output.size()).when(write).getCommittedSize();
    when(write.getFuture()).thenReturn(writtenFuture);

    when(instance.getBlobWrite(
            Compressor.Value.IDENTITY, digest, uuid, RequestMetadata.getDefaultInstance()))
        .thenReturn(write);

    HashCode hash = HashCode.fromString(digest.getHash());
    String resourceName =
        ByteStreamUploader.uploadResourceName(
            /* instanceName=*/ null, uuid, hash, digest.getSizeBytes());

    Channel channel = InProcessChannelBuilder.forName(fakeServerName).directExecutor().build();
    ByteStreamStub service = ByteStreamGrpc.newStub(channel);
    FutureWriteResponseObserver futureResponder = new FutureWriteResponseObserver();
    StreamObserver<WriteRequest> requestObserver = service.write(futureResponder);

    ByteString shortContent = content.substring(0, 6);
    requestObserver.onNext(
        WriteRequest.newBuilder()
            .setWriteOffset(0)
            .setResourceName(resourceName)
            .setData(shortContent)
            .build());
    requestObserver.onNext(
        WriteRequest.newBuilder().setWriteOffset(0).setData(content).setFinishWrite(true).build());
    assertThat(futureResponder.get())
        .isEqualTo(WriteResponse.newBuilder().setCommittedSize(content.size()).build());
    requestObserver.onCompleted();
    verify(write, atLeastOnce()).getCommittedSize();
    verify(write, atLeastOnce())
        .getOutput(any(Long.class), any(TimeUnit.class), any(Runnable.class));
    verify(write, times(1)).reset();
    verify(write, times(1)).getFuture();
  }

  @Test
  public void uploadsCanProgressAfterCancellation() throws Exception {
    ByteString content = ByteString.copyFromUtf8("Hello, World!");
    Digest digest = DIGEST_UTIL.compute(content);
    UUID uuid = UUID.randomUUID();

    SettableFuture<Long> writtenFuture = SettableFuture.create();
    ByteString.Output output = ByteString.newOutput((int) digest.getSizeBytes());
    FeedbackOutputStream out =
        new FeedbackOutputStream() {
          @Override
          public void close() {
            if (output.size() == digest.getSizeBytes()) {
              writtenFuture.set(digest.getSizeBytes());
            }
          }

          @Override
          public void write(byte[] b, int off, int len) {
            output.write(b, off, len);
          }

          @Override
          public void write(int b) {
            output.write(b);
          }

          @Override
          public boolean isReady() {
            return true;
          }
        };

    Write write = mock(Write.class);
    when(write.getOutput(any(Long.class), any(TimeUnit.class), any(Runnable.class)))
        .thenReturn(out);
    doAnswer(invocation -> (long) output.size()).when(write).getCommittedSize();
    when(write.getFuture()).thenReturn(writtenFuture);

    when(instance.getBlobWrite(
            Compressor.Value.IDENTITY, digest, uuid, RequestMetadata.getDefaultInstance()))
        .thenReturn(write);

    HashCode hash = HashCode.fromString(digest.getHash());
    String resourceName =
        ByteStreamUploader.uploadResourceName(
            /* instanceName=*/ null, uuid, hash, digest.getSizeBytes());

    Channel channel = InProcessChannelBuilder.forName(fakeServerName).directExecutor().build();
    ByteStreamStub service = ByteStreamGrpc.newStub(channel);

    FutureWriteResponseObserver futureResponder = new FutureWriteResponseObserver();
    StreamObserver<WriteRequest> requestObserver = service.write(futureResponder);
    ByteString shortContent = content.substring(0, 6);
    requestObserver.onNext(
        WriteRequest.newBuilder()
            .setWriteOffset(0)
            .setResourceName(resourceName)
            .setData(shortContent)
            .build());
    requestObserver.onError(Status.CANCELLED.asException());
    assertThat(futureResponder.isDone()).isTrue(); // should be done

    futureResponder = new FutureWriteResponseObserver();
    requestObserver = service.write(futureResponder);
    requestObserver.onNext(
        WriteRequest.newBuilder()
            .setWriteOffset(6)
            .setResourceName(resourceName)
            .setData(content.substring(6))
            .setFinishWrite(true)
            .build());
    assertThat(futureResponder.get())
        .isEqualTo(WriteResponse.newBuilder().setCommittedSize(content.size()).build());
    requestObserver.onCompleted();
    verify(write, atLeastOnce()).getCommittedSize();
    verify(write, atLeastOnce())
        .getOutput(any(Long.class), any(TimeUnit.class), any(Runnable.class));
    verify(write, times(2)).getFuture();
  }

  static class CountingReadObserver implements StreamObserver<ReadResponse> {
    private final List<Integer> sizes = Lists.newArrayList();
    private final ByteString.Output sink = ByteString.newOutput();
    private boolean completed = false;

    @Override
    public void onNext(ReadResponse response) {
      ByteString data = response.getData();
      try {
        data.writeTo(sink);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      sizes.add(data.size());
    }

    @Override
    public void onCompleted() {
      completed = true;
    }

    @Override
    public void onError(Throwable t) {
      t.printStackTrace();
    }

    List<Integer> getSizesList() {
      return sizes;
    }

    boolean isCompleted() {
      return completed;
    }

    ByteString getData() {
      return sink.toByteString();
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void readSlicesLargeChunksFromInstance() throws Exception {
    // pick a large chunk size
    long size = CHUNK_SIZE * 10 + CHUNK_SIZE - 47;
    ByteString content;
    try (ByteString.Output out =
        ByteString.newOutput(
            ByteStreamService.CHUNK_SIZE * 10 + ByteStreamService.CHUNK_SIZE - 47)) {
      for (long i = 0; i < size; i++) {
        out.write((int) (i & 0xff));
      }
      content = out.toByteString();
    }
    Digest digest = DIGEST_UTIL.compute(content);
    String resourceName = "blobs/" + DigestUtil.toString(digest);
    ReadRequest request = ReadRequest.newBuilder().setResourceName(resourceName).build();

    doAnswer(answerVoid((blobDigest, offset, limit, chunkObserver, metadata) -> {}))
        .when(instance)
        .getBlob(
            eq(Compressor.Value.IDENTITY),
            eq(digest),
            eq(request.getReadOffset()),
            eq((long) content.size()),
            any(ServerCallStreamObserver.class),
            eq(RequestMetadata.getDefaultInstance()));
    Channel channel = InProcessChannelBuilder.forName(fakeServerName).directExecutor().build();
    ByteStreamStub service = ByteStreamGrpc.newStub(channel);
    CountingReadObserver readObserver = new CountingReadObserver();
    service.read(request, readObserver);
    ArgumentCaptor<ServerCallStreamObserver<ByteString>> observerCaptor =
        ArgumentCaptor.forClass(ServerCallStreamObserver.class);
    verify(instance, times(1))
        .getBlob(
            eq(Compressor.Value.IDENTITY),
            eq(digest),
            eq(request.getReadOffset()),
            eq((long) content.size()),
            observerCaptor.capture(),
            eq(RequestMetadata.getDefaultInstance()));

    StreamObserver<ByteString> responseObserver = observerCaptor.getValue();
    // supply entire content
    responseObserver.onNext(content);
    responseObserver.onCompleted();

    assertThat(readObserver.isCompleted()).isTrue();
    assertThat(readObserver.getData()).isEqualTo(content);
    List<Integer> sizes = readObserver.getSizesList();
    assertThat(sizes.size()).isEqualTo(11); // 10 + 1 incomplete chunk
    assertThat(
            sizes.stream()
                .filter((responseSize) -> responseSize > CHUNK_SIZE)
                .collect(Collectors.toList()))
        .isEmpty();
  }
}
