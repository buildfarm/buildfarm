// Copyright 2018 The Bazel Authors. All rights reserved.
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

package build.buildfarm.proxy.http;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.v1test.Digest;
import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamGrpc.ByteStreamBlockingStub;
import com.google.bytestream.ByteStreamGrpc.ByteStreamStub;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusRequest;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusResponse;
import com.google.bytestream.ByteStreamProto.ReadRequest;
import com.google.bytestream.ByteStreamProto.ReadResponse;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class ByteStreamServiceTest {
  private final DigestUtil DIGEST_UTIL = new DigestUtil(HashFunction.SHA256);

  private Server fakeServer;
  private String fakeServerName;

  @Mock private SimpleBlobStore simpleBlobStore;

  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.initMocks(this);
    fakeServerName = "fake server for " + getClass();
    fakeServer =
        InProcessServerBuilder.forName(fakeServerName)
            .addService(new ByteStreamService(simpleBlobStore))
            .directExecutor()
            .build()
            .start();
  }

  @After
  public void tearDown() throws Exception {
    fakeServer.shutdownNow();
    fakeServer.awaitTermination();
  }

  static String createBlobUploadResourceName(String id, Digest digest) {
    return createBlobUploadResourceName(/* instanceName= */ "", id, digest);
  }

  static String createBlobUploadResourceName(String instanceName, String id, Digest digest) {
    return String.format(
        "%suploads/%s/blobs/%s",
        instanceName.isEmpty() ? "" : (instanceName + "/"), id, DigestUtil.toString(digest));
  }

  private String createBlobDownloadResourceName(Digest digest) {
    return createBlobDownloadResourceName(/* instanceName= */ "", digest);
  }

  private String createBlobDownloadResourceName(String instanceName, Digest digest) {
    return String.format(
        "%sblobs/%s",
        instanceName.isEmpty() ? "" : (instanceName + "/"), DigestUtil.toString(digest));
  }

  @Test
  public void missingWriteQueryIsNotFound() throws IOException {
    ByteString helloWorld = ByteString.copyFromUtf8("Hello, World!");
    Digest digest = DIGEST_UTIL.compute(helloWorld);
    String uuid = UUID.randomUUID().toString();
    String resourceName = createBlobUploadResourceName(uuid, digest);

    Channel channel = InProcessChannelBuilder.forName(fakeServerName).directExecutor().build();
    ByteStreamBlockingStub service = ByteStreamGrpc.newBlockingStub(channel);

    StatusRuntimeException notFoundException = null;
    try {
      service.queryWriteStatus(
          QueryWriteStatusRequest.newBuilder().setResourceName(resourceName).build());
    } catch (StatusRuntimeException e) {
      assertThat(Status.fromThrowable(e).getCode()).isEqualTo(Code.NOT_FOUND);
      notFoundException = e;
    }
    assertThat(notFoundException).isNotNull();
  }

  @Test
  public void completedWriteQueryIsFound() throws IOException, InterruptedException {
    ByteString helloWorld = ByteString.copyFromUtf8("Hello, World!");
    Digest digest = DIGEST_UTIL.compute(helloWorld);
    String uuid = UUID.randomUUID().toString();
    String resourceName = createBlobUploadResourceName(uuid, digest);

    when(simpleBlobStore.containsKey(digest.getHash())).thenReturn(true);

    Channel channel = InProcessChannelBuilder.forName(fakeServerName).directExecutor().build();
    ByteStreamBlockingStub service = ByteStreamGrpc.newBlockingStub(channel);
    QueryWriteStatusResponse response =
        service.queryWriteStatus(
            QueryWriteStatusRequest.newBuilder().setResourceName(resourceName).build());
    assertThat(response)
        .isEqualTo(
            QueryWriteStatusResponse.newBuilder()
                .setCommittedSize(digest.getSize())
                .setComplete(true)
                .build());
    verify(simpleBlobStore, times(1)).containsKey(eq(digest.getHash()));
  }

  @Test
  public void writePutsIntoBlobStore() throws IOException, InterruptedException {
    ByteString helloWorld = ByteString.copyFromUtf8("Hello, World!");
    Digest digest = DIGEST_UTIL.compute(helloWorld);
    String uuid = UUID.randomUUID().toString();
    String resourceName = createBlobUploadResourceName(uuid, digest);

    Channel channel = InProcessChannelBuilder.forName(fakeServerName).directExecutor().build();
    ClientCall<WriteRequest, WriteResponse> call =
        channel.newCall(ByteStreamGrpc.getWriteMethod(), CallOptions.DEFAULT);
    ClientCall.Listener<WriteResponse> callListener =
        new ClientCall.Listener<WriteResponse>() {
          boolean complete = false;
          boolean callHalfClosed = false;

          @Override
          public void onReady() {
            while (call.isReady()) {
              if (complete) {
                if (!callHalfClosed) {
                  call.halfClose();
                  callHalfClosed = true;
                }
                return;
              }

              call.sendMessage(
                  WriteRequest.newBuilder()
                      .setResourceName(resourceName)
                      .setData(helloWorld)
                      .setFinishWrite(true)
                      .build());
              complete = true;
            }
          }
        };

    call.start(callListener, new Metadata());
    call.request(1);

    verify(simpleBlobStore, times(1))
        .put(eq(digest.getHash()), eq(digest.getSize()), any(InputStream.class));
  }

  @Test
  public void writeCanBeResumed() throws IOException, InterruptedException {
    ByteString helloWorld = ByteString.copyFromUtf8("Hello, World!");
    Digest digest = DIGEST_UTIL.compute(helloWorld);
    String uuid = UUID.randomUUID().toString();
    String resourceName = createBlobUploadResourceName(uuid, digest);

    Channel channel = InProcessChannelBuilder.forName(fakeServerName).directExecutor().build();
    ClientCall<WriteRequest, WriteResponse> initialCall =
        channel.newCall(ByteStreamGrpc.getWriteMethod(), CallOptions.DEFAULT);
    ByteString initialData = helloWorld.substring(0, 6);
    ClientCall.Listener<WriteResponse> initialCallListener =
        new ClientCall.Listener<WriteResponse>() {
          boolean complete = false;
          boolean callHalfClosed = false;

          @Override
          public void onReady() {
            while (initialCall.isReady()) {
              if (complete) {
                if (!callHalfClosed) {
                  initialCall.halfClose();
                  callHalfClosed = true;
                }
                return;
              }

              initialCall.sendMessage(
                  WriteRequest.newBuilder()
                      .setResourceName(resourceName)
                      .setData(initialData)
                      .build());
              complete = true;
            }
          }
        };

    initialCall.start(initialCallListener, new Metadata());
    initialCall.request(1);

    ByteStreamBlockingStub service = ByteStreamGrpc.newBlockingStub(channel);
    QueryWriteStatusResponse response =
        service.queryWriteStatus(
            QueryWriteStatusRequest.newBuilder().setResourceName(resourceName).build());
    assertThat(response.getCommittedSize()).isEqualTo(initialData.size());
    assertThat(response.getComplete()).isFalse();

    ClientCall<WriteRequest, WriteResponse> finishCall =
        channel.newCall(ByteStreamGrpc.getWriteMethod(), CallOptions.DEFAULT);
    ClientCall.Listener<WriteResponse> finishCallListener =
        new ClientCall.Listener<WriteResponse>() {
          boolean complete = false;
          boolean callHalfClosed = false;

          @Override
          public void onReady() {
            while (finishCall.isReady()) {
              if (complete) {
                if (!callHalfClosed) {
                  finishCall.halfClose();
                  callHalfClosed = true;
                }
                return;
              }

              finishCall.sendMessage(
                  WriteRequest.newBuilder()
                      .setResourceName(resourceName)
                      .setWriteOffset(initialData.size())
                      .setData(helloWorld.substring(initialData.size()))
                      .setFinishWrite(true)
                      .build());
              complete = true;
            }
          }
        };

    finishCall.start(finishCallListener, new Metadata());
    finishCall.request(1);

    ArgumentCaptor<InputStream> inputStreamCaptor = ArgumentCaptor.forClass(InputStream.class);

    verify(simpleBlobStore, times(1))
        .put(eq(digest.getHash()), eq(digest.getSize()), inputStreamCaptor.capture());
    InputStream inputStream = inputStreamCaptor.getValue();
    assertThat(inputStream.available()).isEqualTo(helloWorld.size());
    byte[] data = new byte[helloWorld.size()];
    assertThat(inputStream.read(data)).isEqualTo(helloWorld.size());
    assertThat(data).isEqualTo(helloWorld.toByteArray());
  }

  @Test
  public void missingBlobReadIsNotFound() {
    ByteString helloWorld = ByteString.copyFromUtf8("Hello, World!");
    Digest digest = DIGEST_UTIL.compute(helloWorld);

    Channel channel = InProcessChannelBuilder.forName(fakeServerName).directExecutor().build();
    ByteStreamBlockingStub service = ByteStreamGrpc.newBlockingStub(channel);

    when(simpleBlobStore.get(eq(digest.getHash()), any(OutputStream.class)))
        .thenReturn(immediateFuture(false));
    ReadRequest request =
        ReadRequest.newBuilder().setResourceName(createBlobDownloadResourceName(digest)).build();
    StatusRuntimeException notFoundException = null;
    try {
      if (service.read(request).hasNext()) {
        fail("no responses should be available");
      }
    } catch (StatusRuntimeException e) {
      assertThat(Status.fromThrowable(e).getCode()).isEqualTo(Code.NOT_FOUND);
      notFoundException = e;
    }
    assertThat(notFoundException).isNotNull();
  }

  @Test
  public void skippedInputIsNotInResponse()
      throws ExecutionException, IOException, InterruptedException {
    ByteString helloWorld = ByteString.copyFromUtf8("Hello, World!");
    Digest digest = DIGEST_UTIL.compute(helloWorld);

    Channel channel = InProcessChannelBuilder.forName(fakeServerName).directExecutor().build();
    ByteStreamStub service = ByteStreamGrpc.newStub(channel);

    SettableFuture<Boolean> getComplete = SettableFuture.create();
    when(simpleBlobStore.get(eq(digest.getHash()), any(OutputStream.class)))
        .thenReturn(getComplete);
    ArgumentCaptor<OutputStream> outputStreamCaptor = ArgumentCaptor.forClass(OutputStream.class);

    ReadRequest request =
        ReadRequest.newBuilder()
            .setResourceName(createBlobDownloadResourceName(digest))
            .setReadOffset(6)
            .build();
    SettableFuture<ByteString> readComplete = SettableFuture.create();
    service.read(
        request,
        new StreamObserver<ReadResponse>() {
          ByteString content = ByteString.EMPTY;

          @Override
          public void onNext(ReadResponse response) {
            content = content.concat(response.getData());
          }

          @Override
          public void onError(Throwable t) {
            readComplete.setException(t);
          }

          @Override
          public void onCompleted() {
            readComplete.set(content);
          }
        });

    verify(simpleBlobStore, times(1)).get(eq(digest.getHash()), outputStreamCaptor.capture());
    try (OutputStream outputStream = outputStreamCaptor.getValue()) {
      outputStream.write(helloWorld.toByteArray());
      getComplete.set(true);
    }
    assertThat(readComplete.get()).isEqualTo(helloWorld.substring(6));
  }
}
