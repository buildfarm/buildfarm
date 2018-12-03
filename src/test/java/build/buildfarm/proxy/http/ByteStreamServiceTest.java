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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamGrpc.ByteStreamBlockingStub;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusRequest;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.CallOptions;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.After;
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

  @Mock
  private SimpleBlobStore simpleBlobStore;

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

  private String createBlobUploadResourceName(String id, Digest digest) {
    return String.format(
        "uploads/%s/blobs/%s",
        id,
        DigestUtil.toString(digest));
  }

  /*
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
      service.queryWriteStatus(QueryWriteStatusRequest.newBuilder()
          .setResourceName(resourceName)
          .build());
    } catch (StatusRuntimeException e) {
      notFoundException = e;
    }
    assertThat(notFoundException).isNotNull();
    assertThat(Status.fromThrowable(notFoundException).getCode()).isEqualTo(Code.NOT_FOUND);
  }

  @Test
  public void writePutsIntoBlobStore() throws IOException, InterruptedException {
    ByteString helloWorld = ByteString.copyFromUtf8("Hello, World!");
    Digest digest = DIGEST_UTIL.compute(helloWorld);
    String uuid = UUID.randomUUID().toString();
    String resourceName = createBlobUploadResourceName(uuid, digest);

    Channel channel = InProcessChannelBuilder.forName(fakeServerName).directExecutor().build();
    ClientCall<WriteRequest, WriteResponse> call =
        channel.newCall(ByteStreamGrpc.METHOD_WRITE, CallOptions.DEFAULT);
    ClientCall.Listener<WriteResponse> callListener = new ClientCall.Listener<WriteResponse>() {
      @Override
      public void onReady() {
        call.sendMessage(WriteRequest.newBuilder()
            .setResourceName(resourceName)
            .setData(helloWorld)
            .setFinishWrite(true)
            .build());
        call.halfClose();
      }
    };

    call.start(callListener, new Metadata());
    call.request(1);

    verify(simpleBlobStore, times(1)).put(
        eq(digest.getHash()),
        eq(digest.getSizeBytes()),
        any(InputStream.class));
  }
  */

  @Test
  public void writeCanBeResumed() throws IOException, InterruptedException {
    ByteString helloWorld = ByteString.copyFromUtf8("Hello, World!");
    Digest digest = DIGEST_UTIL.compute(helloWorld);
    String uuid = UUID.randomUUID().toString();
    String resourceName = createBlobUploadResourceName(uuid, digest);

    Channel channel = InProcessChannelBuilder.forName(fakeServerName).directExecutor().build();
    ClientCall<WriteRequest, WriteResponse> initialCall =
        channel.newCall(ByteStreamGrpc.METHOD_WRITE, CallOptions.DEFAULT);
    ClientCall.Listener<WriteResponse> initialCallListener = new ClientCall.Listener<WriteResponse>() {
      @Override
      public void onReady() {
        initialCall.sendMessage(WriteRequest.newBuilder()
            .setResourceName(resourceName)
            .setData(helloWorld.substring(0, 6))
            .build());
        initialCall.halfClose();
      }
    };

    initialCall.start(initialCallListener, new Metadata());
    initialCall.request(1);

    ClientCall<WriteRequest, WriteResponse> finishCall =
        channel.newCall(ByteStreamGrpc.METHOD_WRITE, CallOptions.DEFAULT);
    ClientCall.Listener<WriteResponse> finishCallListener = new ClientCall.Listener<WriteResponse>() {
      @Override
      public void onReady() {
        finishCall.sendMessage(WriteRequest.newBuilder()
            .setResourceName(resourceName)
            .setWriteOffset(6)
            .setData(helloWorld.substring(6))
            .setFinishWrite(true)
            .build());
        finishCall.halfClose();
      }
    };

    finishCall.start(finishCallListener, new Metadata());
    finishCall.request(1);

    ArgumentCaptor<InputStream> inputStreamCaptor = ArgumentCaptor.forClass(InputStream.class);

    verify(simpleBlobStore, times(1)).put(
        eq(digest.getHash()),
        eq(digest.getSizeBytes()),
        inputStreamCaptor.capture());
    InputStream inputStream = inputStreamCaptor.getValue();
    assertThat(inputStream.available()).isEqualTo(helloWorld.size());
    byte[] data = new byte[helloWorld.size()];
    assertThat(inputStream.read(data)).isEqualTo(helloWorld.size());
    assertThat(data).isEqualTo(helloWorld.toByteArray());
  }
}
