// Copyright 2020 The Bazel Authors. All rights reserved.
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

import static build.buildfarm.common.grpc.Retrier.NO_RETRIES;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamGrpc.ByteStreamImplBase;
import com.google.bytestream.ByteStreamProto.ReadRequest;
import com.google.bytestream.ByteStreamProto.ReadResponse;
import com.google.common.base.Suppliers;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ByteStreamHelperTest {
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private final ByteStreamImplBase serviceImpl = mock(ByteStreamImplBase.class);

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
  public void newInputThrowsOnNotFound() {
    String resourceName = "not/found/resource";
    ReadRequest readRequest = ReadRequest.newBuilder().setResourceName(resourceName).build();
    doAnswer(
            invocation -> {
              StreamObserver<ReadResponse> observer = invocation.getArgument(1);
              observer.onError(Status.NOT_FOUND.asException());
              return null;
            })
        .when(serviceImpl)
        .read(eq(readRequest), any(StreamObserver.class));

    try (InputStream in =
        ByteStreamHelper.newInput(
            resourceName,
            /* offset=*/ 0,
            Suppliers.ofInstance(ByteStreamGrpc.newStub(channel)),
            NO_RETRIES::newBackoff,
            NO_RETRIES::isRetriable,
            /* retryService=*/ null)) {
      fail("should not get here");
    } catch (IOException e) {
      assertThat(e).isInstanceOf(NoSuchFileException.class);
    }

    verify(serviceImpl, times(1)).read(eq(readRequest), any(StreamObserver.class));
  }
}
