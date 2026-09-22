// Copyright 2020 The Buildfarm Authors. All rights reserved.
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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamGrpc.ByteStreamImplBase;
import com.google.bytestream.ByteStreamProto.ReadRequest;
import com.google.bytestream.ByteStreamProto.ReadResponse;
import com.google.common.base.Suppliers;
import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

@RunWith(JUnit4.class)
public class ByteStreamHelperTest {
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  @Spy private ByteStreamImplBase serviceImpl;

  private Channel channel;
  private ExecutorService clientExecutor;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    String serverName = InProcessServerBuilder.generateName();

    grpcCleanup
        .register(
            InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(serviceImpl)
                .build())
        .start();

    clientExecutor = Executors.newCachedThreadPool();
    channel =
        grpcCleanup.register(
            InProcessChannelBuilder.forName(serverName).executor(clientExecutor).build());
  }

  @After
  public void tearDown() {
    clientExecutor.shutdownNow();
  }

  private InputStream newInput(String resourceName) throws IOException {
    return ByteStreamHelper.newInput(
        resourceName,
        /* offset= */ 0,
        "test-endpoint",
        Suppliers.ofInstance(ByteStreamGrpc.newStub(channel)),
        NO_RETRIES::newBackoff,
        NO_RETRIES::isRetriable,
        /* retryService= */ null);
  }

  // Serves an unbounded stream of chunks as fast as the client requests them, tracking
  // cancellation and the number of chunks sent. The stream never completes on its own.
  private static final class EndlessRead {
    final CountDownLatch cancelled = new CountDownLatch(1);
    final AtomicInteger chunksSent = new AtomicInteger();

    void serve(ServerCallStreamObserver<ReadResponse> observer) {
      observer.setOnCancelHandler(cancelled::countDown);
      observer.setOnReadyHandler(
          () -> {
            while (observer.isReady() && !observer.isCancelled()) {
              observer.onNext(
                  ReadResponse.newBuilder().setData(ByteString.copyFromUtf8("chunk")).build());
              chunksSent.incrementAndGet();
            }
          });
    }
  }

  @SuppressWarnings("unchecked")
  private EndlessRead serveEndlessRead(String resourceName) {
    EndlessRead read = new EndlessRead();
    doAnswer(
            invocation -> {
              read.serve((ServerCallStreamObserver<ReadResponse>) invocation.getArgument(1));
              return null;
            })
        .when(serviceImpl)
        .read(
            eq(ReadRequest.newBuilder().setResourceName(resourceName).build()),
            any(StreamObserver.class));
    return read;
  }

  private void assertClientCallbacksQuiesce() throws InterruptedException {
    clientExecutor.shutdown();
    assertThat(clientExecutor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
  }

  @Test
  public void closeCancelsReadAndReleasesProducer() throws Exception {
    String resourceName = "endless/resource";
    EndlessRead read = serveEndlessRead(resourceName);

    try (InputStream in = newInput(resourceName)) {
      assertThat(in.read()).isEqualTo('c');
    }

    assertThat(read.cancelled.await(10, TimeUnit.SECONDS)).isTrue();
    assertClientCallbacksQuiesce();
  }

  @Test
  public void interruptedReadThenCloseCancelsReadAndReleasesProducer() throws Exception {
    String resourceName = "endless/resource";
    EndlessRead read = serveEndlessRead(resourceName);

    InputStream in = newInput(resourceName);
    CountDownLatch reading = new CountDownLatch(1);
    Thread reader =
        new Thread(
            () -> {
              try (InputStream stream = in) {
                byte[] buf = new byte[1 << 20];
                while (true) {
                  reading.countDown();
                  if (stream.read(buf) == -1) {
                    break;
                  }
                }
              } catch (IOException e) {
                // expected: interrupted while waiting for a chunk
              }
            });
    reader.start();
    assertThat(reading.await(10, TimeUnit.SECONDS)).isTrue();
    reader.interrupt();
    reader.join(TimeUnit.SECONDS.toMillis(10));
    assertThat(reader.isAlive()).isFalse();

    assertThat(read.cancelled.await(10, TimeUnit.SECONDS)).isTrue();
    assertClientCallbacksQuiesce();
  }

  @Test
  public void readIsFlowControlledByConsumer() throws Exception {
    String resourceName = "endless/resource";
    EndlessRead read = serveEndlessRead(resourceName);

    try (InputStream in = newInput(resourceName)) {
      assertThat(in.read()).isEqualTo('c');
      // give the producer every opportunity to run ahead of the consumer
      Thread.sleep(200);
      assertThat(read.chunksSent.get()).isAtMost(2);
    }
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

    try (InputStream ignored =
        ByteStreamHelper.newInput(
            resourceName,
            /* offset= */ 0,
            "test-endpoint",
            Suppliers.ofInstance(ByteStreamGrpc.newStub(channel)),
            NO_RETRIES::newBackoff,
            NO_RETRIES::isRetriable,
            /* retryService= */ null)) {
      fail("should not get here");
    } catch (IOException e) {
      assertThat(e).isInstanceOf(NoSuchFileException.class);
    }

    verify(serviceImpl, times(1)).read(eq(readRequest), any(StreamObserver.class));
  }
}
