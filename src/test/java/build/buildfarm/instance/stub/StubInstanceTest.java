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

package build.buildfarm.instance.stub;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.truth.Truth.assertThat;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionCacheGrpc.ActionCacheImplBase;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.BatchUpdateBlobsRequest;
import build.bazel.remote.execution.v2.BatchUpdateBlobsResponse;
import build.bazel.remote.execution.v2.BatchUpdateBlobsResponse.Response;
import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc.ContentAddressableStorageImplBase;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.FindMissingBlobsRequest;
import build.bazel.remote.execution.v2.FindMissingBlobsResponse;
import build.bazel.remote.execution.v2.GetActionResultRequest;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.bazel.remote.execution.v2.UpdateActionResultRequest;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.Write;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.StubInstance.ReadBlobInterchange;
import com.google.bytestream.ByteStreamGrpc.ByteStreamImplBase;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusRequest;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusResponse;
import com.google.bytestream.ByteStreamProto.ReadRequest;
import com.google.bytestream.ByteStreamProto.ReadResponse;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.grpc.util.MutableHandlerRegistry;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

@RunWith(JUnit4.class)
public class StubInstanceTest {
  private static final DigestUtil DIGEST_UTIL = new DigestUtil(DigestUtil.HashFunction.SHA256);

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

  private StubInstance newStubInstance(String instanceName) {
    return new StubInstance(
        instanceName,
        DIGEST_UTIL,
        InProcessChannelBuilder.forName(fakeServerName).directExecutor().build());
  }

  @Test
  public void reflectsNameAndDigestUtil() {
    String test1Name = "test1";
    ByteString test1Blob = ByteString.copyFromUtf8(test1Name);
    DigestUtil test1DigestUtil = new DigestUtil(DigestUtil.HashFunction.SHA256);
    Instance test1Instance = new StubInstance(test1Name, test1DigestUtil, /* channel=*/ null);
    assertThat(test1Instance.getName()).isEqualTo(test1Name);
    assertThat(test1Instance.getDigestUtil().compute(test1Blob))
        .isEqualTo(test1DigestUtil.compute(test1Blob));

    /* and once again to verify that those values change due to inputs */
    String test2Name = "test2";
    ByteString test2Blob = ByteString.copyFromUtf8(test2Name);
    DigestUtil test2DigestUtil = new DigestUtil(DigestUtil.HashFunction.MD5);
    Instance test2Instance = new StubInstance(test2Name, test2DigestUtil, /* channel=*/ null);
    assertThat(test2Instance.getName()).isEqualTo(test2Name);
    assertThat(test2Instance.getDigestUtil().compute(test2Blob))
        .isEqualTo(test2DigestUtil.compute(test2Blob));
  }

  @Test
  public void getActionResultReturnsNullForNotFound() throws Exception {
    AtomicReference<GetActionResultRequest> reference = new AtomicReference<>();
    serviceRegistry.addService(
        new ActionCacheImplBase() {
          @Override
          public void getActionResult(
              GetActionResultRequest request, StreamObserver<ActionResult> responseObserver) {
            reference.set(request);
            responseObserver.onError(Status.NOT_FOUND.asException());
          }
        });
    Instance instance = newStubInstance("test");
    ActionKey actionKey = DIGEST_UTIL.computeActionKey(Action.getDefaultInstance());
    assertThat(instance.getActionResult(actionKey, RequestMetadata.getDefaultInstance()).get())
        .isNull();
    GetActionResultRequest request = reference.get();
    assertThat(request.getInstanceName()).isEqualTo(instance.getName());
    assertThat(request.getActionDigest()).isEqualTo(actionKey.getDigest());
    instance.stop();
  }

  @Test
  public void putActionResultCallsUpdateActionResult() throws InterruptedException {
    AtomicReference<UpdateActionResultRequest> reference = new AtomicReference<>();
    serviceRegistry.addService(
        new ActionCacheImplBase() {
          @Override
          public void updateActionResult(
              UpdateActionResultRequest request, StreamObserver<ActionResult> responseObserver) {
            reference.set(request);
            responseObserver.onNext(request.getActionResult());
            responseObserver.onCompleted();
          }
        });
    String instanceName = "putActionResult-test";
    Instance instance = newStubInstance(instanceName);
    ActionKey actionKey =
        DigestUtil.asActionKey(
            Digest.newBuilder().setHash("action-digest").setSizeBytes(1).build());
    ActionResult actionResult = ActionResult.getDefaultInstance();
    instance.putActionResult(actionKey, actionResult);
    UpdateActionResultRequest request = reference.get();
    assertThat(request.getInstanceName()).isEqualTo(instanceName);
    assertThat(request.getActionDigest()).isEqualTo(actionKey.getDigest());
    assertThat(request.getActionResult()).isEqualTo(actionResult);
    instance.stop();
  }

  @Test
  public void findMissingBlobsCallsFindMissingBlobs()
      throws ExecutionException, InterruptedException {
    AtomicReference<FindMissingBlobsRequest> reference = new AtomicReference<>();
    serviceRegistry.addService(
        new ContentAddressableStorageImplBase() {
          @Override
          public void findMissingBlobs(
              FindMissingBlobsRequest request,
              StreamObserver<FindMissingBlobsResponse> responseObserver) {
            reference.set(request);
            responseObserver.onNext(FindMissingBlobsResponse.getDefaultInstance());
            responseObserver.onCompleted();
          }
        });
    Instance instance = newStubInstance("findMissingBlobs-test");
    Iterable<Digest> digests =
        ImmutableList.of(Digest.newBuilder().setHash("present").setSizeBytes(1).build());
    assertThat(instance.findMissingBlobs(digests, RequestMetadata.getDefaultInstance()).get())
        .isEmpty();
    instance.stop();
  }

  @Test
  public void findMissingBlobsOverSizeLimitRecombines()
      throws ExecutionException, InterruptedException {
    AtomicReference<FindMissingBlobsRequest> reference = new AtomicReference<>();
    serviceRegistry.addService(
        new ContentAddressableStorageImplBase() {
          @Override
          public void findMissingBlobs(
              FindMissingBlobsRequest request,
              StreamObserver<FindMissingBlobsResponse> responseObserver) {
            reference.set(request);
            responseObserver.onNext(
                FindMissingBlobsResponse.newBuilder()
                    .addAllMissingBlobDigests(request.getBlobDigestsList())
                    .build());
            responseObserver.onCompleted();
          }
        });
    StubInstance instance = newStubInstance("findMissingBlobs-test");
    instance.maxRequestSize = 1024;
    ImmutableList.Builder<Digest> builder = ImmutableList.builder();
    // generates digest size * 1024 serialized size at least
    for (int i = 0; i < 1024; i++) {
      ByteString content = ByteString.copyFromUtf8("Hello, World! " + UUID.randomUUID());
      builder.add(DIGEST_UTIL.compute(content));
    }
    ImmutableList<Digest> digests = builder.build();
    assertThat(instance.findMissingBlobs(digests, RequestMetadata.getDefaultInstance()).get())
        .containsExactlyElementsIn(digests);
    instance.stop();
  }

  @Test
  public void outputStreamWrites() throws IOException, InterruptedException {
    AtomicReference<ByteString> writtenContent = new AtomicReference<>();
    serviceRegistry.addService(
        new ByteStreamImplBase() {
          ByteString content = ByteString.EMPTY;
          boolean finished = false;

          public void queryWriteStatus(
              QueryWriteStatusRequest request,
              StreamObserver<QueryWriteStatusResponse> responseObserver) {
            responseObserver.onNext(
                QueryWriteStatusResponse.newBuilder()
                    .setCommittedSize(content.size())
                    .setComplete(finished)
                    .build());
            responseObserver.onCompleted();
          }

          @Override
          public StreamObserver<WriteRequest> write(
              StreamObserver<WriteResponse> responseObserver) {
            return new StreamObserver<WriteRequest>() {
              @Override
              public void onNext(WriteRequest request) {
                checkState(!finished);
                if (request.getData().size() != 0) {
                  checkState(request.getWriteOffset() == content.size());
                  content = content.concat(request.getData());
                }
                finished = request.getFinishWrite();
                if (finished) {
                  writtenContent.set(content);
                  responseObserver.onNext(
                      WriteResponse.newBuilder().setCommittedSize(content.size()).build());
                }
              }

              @Override
              public void onError(Throwable t) {
                t.printStackTrace();
              }

              @Override
              public void onCompleted() {
                responseObserver.onCompleted();
              }
            };
          }
        });
    Instance instance = newStubInstance("outputStream-test");
    String resourceName = "output-stream-test";
    ByteString content = ByteString.copyFromUtf8("test-content");
    Write operationStreamWrite = instance.getOperationStreamWrite(resourceName);
    try (OutputStream out = operationStreamWrite.getOutput(1, SECONDS, () -> {})) {
      content.writeTo(out);
    }
    assertThat(writtenContent.get()).isEqualTo(content);
    instance.stop();
  }

  @Test
  public void putAllBlobsUploadsBlobs() throws Exception {
    String instanceName = "putAllBlobs-test";
    serviceRegistry.addService(
        new ContentAddressableStorageImplBase() {
          @Override
          public void batchUpdateBlobs(
              BatchUpdateBlobsRequest batchRequest,
              StreamObserver<BatchUpdateBlobsResponse> responseObserver) {
            checkState(batchRequest.getInstanceName().equals(instanceName));
            responseObserver.onNext(
                BatchUpdateBlobsResponse.newBuilder()
                    .addAllResponses(
                        batchRequest.getRequestsList().stream()
                            .map(
                                request ->
                                    Response.newBuilder().setDigest(request.getDigest()).build())
                            .collect(Collectors.toList()))
                    .build());
            responseObserver.onCompleted();
          }
        });
    Instance instance = newStubInstance("putAllBlobs-test");
    ByteString first = ByteString.copyFromUtf8("first");
    ByteString last = ByteString.copyFromUtf8("last");
    ImmutableList<ByteString> blobs = ImmutableList.of(first, last);
    ImmutableList<Digest> digests =
        ImmutableList.of(DIGEST_UTIL.compute(first), DIGEST_UTIL.compute(last));
    assertThat(instance.putAllBlobs(blobs, RequestMetadata.getDefaultInstance()))
        .containsAtLeastElementsIn(digests);
  }

  @Test
  public void completedWriteBeforeCloseThrowsOnNextInteraction()
      throws IOException, InterruptedException {
    String resourceName = "early-completed-output-stream-test";
    serviceRegistry.addService(
        new ByteStreamImplBase() {
          boolean completed = false;
          int writtenBytes = 0;

          @Override
          public StreamObserver<WriteRequest> write(
              StreamObserver<WriteResponse> responseObserver) {
            return new StreamObserver<WriteRequest>() {
              @Override
              public void onNext(WriteRequest request) {
                if (!completed) {
                  writtenBytes = request.getData().size();
                  responseObserver.onNext(
                      WriteResponse.newBuilder().setCommittedSize(writtenBytes).build());
                  responseObserver.onCompleted();
                  completed = true;
                }
              }

              @Override
              public void onError(Throwable t) {
                t.printStackTrace();
              }

              @Override
              public void onCompleted() {}
            };
          }

          @Override
          public void queryWriteStatus(
              QueryWriteStatusRequest request,
              StreamObserver<QueryWriteStatusResponse> responseObserver) {
            if (request.getResourceName().equals(resourceName)) {
              responseObserver.onNext(
                  QueryWriteStatusResponse.newBuilder()
                      .setCommittedSize(writtenBytes)
                      .setComplete(completed)
                      .build());
              responseObserver.onCompleted();
            } else {
              responseObserver.onError(Status.NOT_FOUND.asException());
            }
          }
        });
    Instance instance = newStubInstance("early-completed-outputStream-test");
    ByteString content = ByteString.copyFromUtf8("test-content");
    boolean writeThrewException = false;
    Write operationStreamWrite = instance.getOperationStreamWrite(resourceName);
    try (OutputStream out = operationStreamWrite.getOutput(1, SECONDS, () -> {})) {
      content.writeTo(out);
      try {
        content.writeTo(out);
      } catch (Exception e) {
        writeThrewException = true;
      }
    }
    assertThat(writeThrewException).isTrue();
    instance.stop();
  }

  @Test
  public void inputStreamThrowsNonDeadlineExceededCausal()
      throws IOException, InterruptedException {
    serviceRegistry.addService(
        new ByteStreamImplBase() {
          @Override
          public void read(ReadRequest request, StreamObserver<ReadResponse> responseObserver) {
            responseObserver.onError(Status.UNAVAILABLE.asException());
          }
        });
    OutputStream out = mock(OutputStream.class);
    IOException ioException = null;
    Instance instance = newStubInstance("input-stream-non-deadline-exceeded");
    Digest unavailableDigest =
        Digest.newBuilder().setHash("unavailable-blob-name").setSizeBytes(1).build();
    try (InputStream in =
        instance.newBlobInput(
            Compressor.Value.IDENTITY,
            unavailableDigest,
            0,
            1,
            SECONDS,
            RequestMetadata.getDefaultInstance())) {
      ByteStreams.copy(in, out);
    } catch (IOException e) {
      ioException = e;
    }
    assertThat(ioException).isNotNull();
    Status status = Status.fromThrowable(ioException);
    assertThat(status.getCode()).isEqualTo(Code.UNAVAILABLE);
    verifyNoInteractions(out);
    instance.stop();
  }

  @Test
  public void inputStreamRetriesOnDeadlineExceededWithProgress()
      throws IOException, InterruptedException {
    ByteString content = ByteString.copyFromUtf8("1");
    serviceRegistry.addService(
        new ByteStreamImplBase() {
          boolean first = true;

          @Override
          public void read(ReadRequest request, StreamObserver<ReadResponse> responseObserver) {
            if (first && request.getReadOffset() == 0) {
              first = false;
              responseObserver.onNext(ReadResponse.newBuilder().setData(content).build());
              responseObserver.onError(Status.DEADLINE_EXCEEDED.asException());
            } else if (request.getReadOffset() == 1) {
              responseObserver.onCompleted();
            } else {
              // all others fail with unimplemented
              responseObserver.onError(Status.UNIMPLEMENTED.asException());
            }
          }
        });
    Instance instance = newStubInstance("input-stream-stalled");
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Digest delayedDigest = Digest.newBuilder().setHash("delayed-blob-name").setSizeBytes(1).build();
    try (InputStream in =
        instance.newBlobInput(
            Compressor.Value.IDENTITY,
            delayedDigest,
            0,
            1,
            SECONDS,
            RequestMetadata.getDefaultInstance())) {
      ByteStreams.copy(in, out);
    }
    assertThat(ByteString.copyFrom(out.toByteArray())).isEqualTo(content);
    instance.stop();
  }

  @Test
  public void inputStreamThrowsOnDeadlineExceededWithoutProgress()
      throws IOException, InterruptedException {
    serviceRegistry.addService(
        new ByteStreamImplBase() {
          @Override
          public void read(ReadRequest request, StreamObserver<ReadResponse> responseObserver) {
            responseObserver.onError(Status.DEADLINE_EXCEEDED.asException());
          }
        });
    OutputStream out = mock(OutputStream.class);
    IOException ioException = null;
    Instance instance = newStubInstance("input-stream-deadline-exceeded");
    Digest timeoutDigest = Digest.newBuilder().setHash("timeout-blob-name").setSizeBytes(1).build();
    try (InputStream in =
        instance.newBlobInput(
            Compressor.Value.IDENTITY,
            timeoutDigest,
            0,
            1,
            SECONDS,
            RequestMetadata.getDefaultInstance())) {
      ByteStreams.copy(in, out);
    } catch (IOException e) {
      ioException = e;
    }
    assertThat(ioException).isNotNull();
    Status status = Status.fromThrowable(ioException);
    assertThat(status.getCode()).isEqualTo(Code.DEADLINE_EXCEEDED);
    verifyNoInteractions(out);
    instance.stop();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void readBlobInterchangeDoesNotRequestUntilStarted() {
    ServerCallStreamObserver<ByteString> mockBlobObserver = mock(ServerCallStreamObserver.class);
    ReadBlobInterchange interchange = new ReadBlobInterchange(mockBlobObserver);

    ClientCallStreamObserver<ReadRequest> mockRequestStream = mock(ClientCallStreamObserver.class);
    interchange.beforeStart(mockRequestStream);
    // verify our flow control call so that we can verify no further interactions
    verify(mockRequestStream, times(1)).disableAutoInboundFlowControl();
    // capture onReady from mockBlobObserver
    ArgumentCaptor<Runnable> onReadyCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(mockBlobObserver, times(1)).setOnReadyHandler(onReadyCaptor.capture());
    // call it
    onReadyCaptor.getValue().run();
    // verify no more interactions with mockRequestStream
    verifyNoMoreInteractions(mockRequestStream);
  }
}
