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

package build.buildfarm.cas;

import static com.google.common.truth.Truth.assertThat;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc.ContentAddressableStorageImplBase;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.cas.ContentAddressableStorage.Blob;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.Write;
import build.buildfarm.common.grpc.ByteStreamServiceWriter;
import build.buildfarm.instance.stub.ByteStreamUploader;
import build.buildfarm.instance.stub.Chunker;
import com.google.bytestream.ByteStreamGrpc.ByteStreamImplBase;
import com.google.bytestream.ByteStreamProto.ReadRequest;
import com.google.bytestream.ByteStreamProto.ReadResponse;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.hash.HashCode;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.util.MutableHandlerRegistry;
import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GrpcCASTest {
  private static final DigestUtil DIGEST_UTIL = new DigestUtil(HashFunction.SHA256);

  private final MutableHandlerRegistry serviceRegistry = new MutableHandlerRegistry();
  private final String fakeServerName = "fake server for " + getClass();
  private Server fakeServer;
  private ListMultimap<Digest, Runnable> onExpirations;

  @Before
  public void setUp() throws IOException {
    // Use a mutable service registry for later registering the service impl for each test case.
    fakeServer =
        InProcessServerBuilder.forName(fakeServerName)
            .fallbackHandlerRegistry(serviceRegistry)
            .directExecutor()
            .build()
            .start();

    onExpirations = MultimapBuilder.hashKeys().arrayListValues().build();
  }

  @After
  public void tearDown() throws InterruptedException {
    fakeServer.shutdownNow();
    fakeServer.awaitTermination();
  }

  @Test
  public void getHandlesNotFound() {
    Digest digest = DIGEST_UTIL.compute(ByteString.copyFromUtf8("nonexistent"));
    String instanceName = "test";
    final AtomicReference<Boolean> readCalled = new AtomicReference<>(false);
    serviceRegistry.addService(
        new ByteStreamImplBase() {
          @Override
          public void read(ReadRequest request, StreamObserver<ReadResponse> responseObserver) {
            assertThat(request.getResourceName())
                .isEqualTo(String.format("%s/blobs/%s", instanceName, DigestUtil.toString(digest)));
            readCalled.compareAndSet(false, true);
            responseObserver.onError(Status.NOT_FOUND.asException());
          }
        });

    GrpcCAS cas =
        new GrpcCAS(
            instanceName,
            /* readonly=*/ true,
            InProcessChannelBuilder.forName(fakeServerName).directExecutor().build(),
            mock(ByteStreamUploader.class),
            onExpirations);
    assertThat(cas.get(digest)).isNull();
    assertThat(readCalled.get()).isTrue();
  }

  @Test
  public void onExpirationCalledWhenNotFound() {
    Digest digest = DIGEST_UTIL.compute(ByteString.copyFromUtf8("nonexistent"));
    String instanceName = "test";
    final AtomicReference<Boolean> readCalled = new AtomicReference<>(false);
    serviceRegistry.addService(
        new ByteStreamImplBase() {
          @Override
          public void read(ReadRequest request, StreamObserver<ReadResponse> responseObserver) {
            assertThat(request.getResourceName())
                .isEqualTo(String.format("%s/blobs/%s", instanceName, DigestUtil.toString(digest)));
            readCalled.compareAndSet(false, true);
            responseObserver.onError(Status.NOT_FOUND.asException());
          }
        });

    Runnable onExpiration = mock(Runnable.class);
    onExpirations.put(digest, onExpiration);

    GrpcCAS cas =
        new GrpcCAS(
            instanceName,
            /* readonly=*/ true,
            InProcessChannelBuilder.forName(fakeServerName).directExecutor().build(),
            mock(ByteStreamUploader.class),
            onExpirations);
    assertThat(cas.get(digest)).isNull();
    assertThat(readCalled.get()).isTrue();
    verify(onExpiration, times(1)).run();
  }

  @Test
  public void putAddsExpiration() throws IOException, InterruptedException {
    ByteString uploadContent = ByteString.copyFromUtf8("uploaded");
    Digest digest = DIGEST_UTIL.compute(uploadContent);
    String instanceName = "test";
    ListMultimap<Digest, Runnable> onExpirations =
        MultimapBuilder.hashKeys().arrayListValues().build();
    Channel channel = InProcessChannelBuilder.forName(fakeServerName).directExecutor().build();
    ByteStreamUploader uploader = mock(ByteStreamUploader.class);
    GrpcCAS cas = new GrpcCAS(instanceName, /* readonly=*/ false, channel, uploader, onExpirations);
    Runnable onExpiration = mock(Runnable.class);
    cas.put(new Blob(uploadContent, digest), onExpiration);
    verify(uploader, times(1))
        .uploadBlob(eq(HashCode.fromString(digest.getHash())), any(Chunker.class));
    assertThat(onExpirations.get(digest)).containsExactly(onExpiration);
    verifyZeroInteractions(onExpiration);
  }

  @Test
  public void writeIsResumable() throws Exception {
    UUID uuid = UUID.randomUUID();
    ByteString writeContent = ByteString.copyFromUtf8("written");
    Digest digest = DIGEST_UTIL.compute(writeContent);
    String instanceName = "test";
    HashCode hash = HashCode.fromString(digest.getHash());
    String resourceName =
        ByteStreamUploader.uploadResourceName(instanceName, uuid, hash, digest.getSizeBytes());

    // better test might just put a full gRPC CAS behind an in-process and validate state
    SettableFuture<ByteString> content = SettableFuture.create();
    serviceRegistry.addService(
        new ByteStreamServiceWriter(resourceName, content, (int) digest.getSizeBytes()));

    Channel channel = InProcessChannelBuilder.forName(fakeServerName).directExecutor().build();
    GrpcCAS cas =
        new GrpcCAS(
            instanceName, /* readonly=*/ false, channel, /* uploader=*/ null, onExpirations);
    RequestMetadata requestMetadata = RequestMetadata.getDefaultInstance();
    Write initialWrite = cas.getWrite(Compressor.Value.IDENTITY, digest, uuid, requestMetadata);
    try (OutputStream writeOut = initialWrite.getOutput(1, SECONDS, () -> {})) {
      writeContent.substring(0, 4).writeTo(writeOut);
    }
    Write finalWrite = cas.getWrite(Compressor.Value.IDENTITY, digest, uuid, requestMetadata);
    try (OutputStream writeOut = finalWrite.getOutput(1, SECONDS, () -> {})) {
      writeContent.substring(4).writeTo(writeOut);
    }
    assertThat(content.get(1, TimeUnit.SECONDS)).isEqualTo(writeContent);
  }

  @Test
  public void writeIsNullForReadonly() throws Exception {
    UUID uuid = UUID.randomUUID();
    ByteString writeContent = ByteString.copyFromUtf8("undesirable");
    Digest digest = DIGEST_UTIL.compute(writeContent);
    String instanceName = "test";
    GrpcCAS cas =
        new GrpcCAS(
            instanceName,
            /* readonly=*/ true,
            /* channel=*/ null,
            /* uploader=*/ null,
            onExpirations);

    RequestMetadata requestMetadata = RequestMetadata.getDefaultInstance();
    Write nullWrite = cas.getWrite(Compressor.Value.IDENTITY, digest, uuid, requestMetadata);
    assertThat(nullWrite).isNull();
  }

  @Test
  public void findMissingBlobsSwallowsFilteredList() throws Exception {
    Channel channel = InProcessChannelBuilder.forName(fakeServerName).directExecutor().build();
    Runnable onExpiration = mock(Runnable.class);
    GrpcCAS cas = new GrpcCAS("test", /* readonly=*/ false, channel, null, onExpirations);
    ContentAddressableStorageImplBase casService = mock(ContentAddressableStorageImplBase.class);
    serviceRegistry.addService(casService);
    Digest emptyDigest = Digest.getDefaultInstance();
    assertThat(cas.findMissingBlobs(ImmutableList.of(emptyDigest))).isEmpty();
    verifyZeroInteractions(casService);
    verifyZeroInteractions(onExpiration);
  }
}
