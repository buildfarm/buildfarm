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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.instance.stub.ByteStreamUploader;
import com.google.bytestream.ByteStreamGrpc.ByteStreamImplBase;
import com.google.bytestream.ByteStreamProto.ReadRequest;
import com.google.bytestream.ByteStreamProto.ReadResponse;
import build.bazel.remote.execution.v2.Digest;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.util.MutableHandlerRegistry;
import java.io.IOException;
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

  @Before
  public void setUp() throws IOException {
    // Use a mutable service registry for later registering the service impl for each test case.
    fakeServer =
        InProcessServerBuilder.forName(fakeServerName)
            .fallbackHandlerRegistry(serviceRegistry)
            .directExecutor()
            .build()
            .start();
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
            assertThat(request.getResourceName()).isEqualTo(String.format("%s/blobs/%s", instanceName, DigestUtil.toString(digest)));
            readCalled.compareAndSet(false, true);
            responseObserver.onError(Status.NOT_FOUND.asException());
          }
        });

    GrpcCAS cas = new GrpcCAS(
        instanceName,
        InProcessChannelBuilder.forName(fakeServerName).directExecutor().build(),
        mock(ByteStreamUploader.class));
    assertThat(cas.get(digest)).isNull();
    assertThat(readCalled.get()).isTrue();
  }
}
