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

package build.buildfarm.instance.stub;

import static build.buildfarm.common.grpc.Retrier.NO_RETRIES;

import com.google.bytestream.ByteStreamGrpc.ByteStreamImplBase;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.common.hash.HashCode;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.util.MutableHandlerRegistry;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ByteStreamUploaderTest {
  private final MutableHandlerRegistry serviceRegistry = new MutableHandlerRegistry();
  private final String fakeServerName = "fake server for " + getClass();
  private Server fakeServer;

  @Before
  public void setUp() throws IOException {
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

  @Test(expected = IOException.class)
  public void uploadBlobPropagatesIOExceptionFromIncompleteUpload()
      throws IOException, InterruptedException {
    serviceRegistry.addService(
        new ByteStreamImplBase() {
          @Override
          public StreamObserver<WriteRequest> write(
              StreamObserver<WriteResponse> responseObserver) {
            responseObserver.onNext(WriteResponse.newBuilder().setCommittedSize(1).build());
            responseObserver.onCompleted();
            return new StreamObserver<WriteRequest>() {
              @Override
              public void onNext(WriteRequest request) {}

              @Override
              public void onError(Throwable t) {}

              @Override
              public void onCompleted() {}
            };
          }
        });
    ByteStreamUploader uploader =
        new ByteStreamUploader(
            /* instanceName= */ null,
            InProcessChannelBuilder.forName(fakeServerName).directExecutor().build(),
            /* callCredentials= */ null,
            /* callTimeoutSecs= */ 1,
            NO_RETRIES);
    Chunker chunker = Chunker.builder().setInput(ByteString.copyFromUtf8("Hello, World!")).build();
    uploader.uploadBlob(HashCode.fromInt(42), chunker);
  }
}
