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

import static build.buildfarm.common.grpc.Retrier.NO_RETRIES;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionCacheGrpc.ActionCacheImplBase;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc.ContentAddressableStorageImplBase;
import build.bazel.remote.execution.v2.FindMissingBlobsRequest;
import build.bazel.remote.execution.v2.FindMissingBlobsResponse;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.UpdateActionResultRequest;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.grpc.ByteStreamServiceWriter;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.ByteStreamUploader;
import com.google.bytestream.ByteStreamGrpc.ByteStreamImplBase;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusRequest;
import com.google.bytestream.ByteStreamProto.QueryWriteStatusResponse;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.util.MutableHandlerRegistry;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

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

  @Test
  public void reflectsNameAndDigestUtil() {
    String test1Name = "test1";
    ByteString test1Blob = ByteString.copyFromUtf8(test1Name);
    DigestUtil test1DigestUtil = new DigestUtil(DigestUtil.HashFunction.SHA256);
    Instance test1Instance = new StubInstance(
        test1Name,
        test1DigestUtil,
        /* channel=*/ null,
        /* uploader=*/ null,
        NO_RETRIES,
        /* retryScheduler=*/ null);
    assertThat(test1Instance.getName()).isEqualTo(test1Name);
    assertThat(test1Instance.getDigestUtil().compute(test1Blob))
        .isEqualTo(test1DigestUtil.compute(test1Blob));

    /* and once again to verify that those values change due to inputs */
    String test2Name = "test2";
    ByteString test2Blob = ByteString.copyFromUtf8(test2Name);
    DigestUtil test2DigestUtil = new DigestUtil(DigestUtil.HashFunction.MD5);
    Instance test2Instance = new StubInstance(
        test2Name,
        test2DigestUtil,
        /* channel=*/ null,
        /* uploader=*/ null,
        NO_RETRIES,
        /* retryScheduler=*/ null);
    assertThat(test2Instance.getName()).isEqualTo(test2Name);
    assertThat(test2Instance.getDigestUtil().compute(test2Blob))
        .isEqualTo(test2DigestUtil.compute(test2Blob));
  }

  @Test
  public void getActionResultReturnsNull() {
    Instance instance = new StubInstance(
        "test",
        DIGEST_UTIL,
        /* channel=*/ null,
        /* uploader=*/ null,
        NO_RETRIES,
        /* retryScheduler=*/ null);
    assertThat(instance.getActionResult(DIGEST_UTIL.computeActionKey(Action.getDefaultInstance()))).isNull();
  }

  @Test
  public void putActionResultCallsUpdateActionResult() throws InterruptedException {
    AtomicReference<UpdateActionResultRequest> reference = new AtomicReference<>();
    serviceRegistry.addService(
        new ActionCacheImplBase() {
          @Override
          public void updateActionResult(UpdateActionResultRequest request, StreamObserver<ActionResult> responseObserver) {
            reference.set(request);
            responseObserver.onNext(request.getActionResult());
            responseObserver.onCompleted();
          }
        });
    String instanceName = "putActionResult-test";
    Instance instance = new StubInstance(
        instanceName,
        DIGEST_UTIL,
        InProcessChannelBuilder.forName(fakeServerName).directExecutor().build(),
        /* uploader=*/ null,
        NO_RETRIES,
        /* retryScheduler=*/ null);
    ActionKey actionKey = DigestUtil.asActionKey(Digest.newBuilder()
        .setHash("action-digest")
        .setSizeBytes(1)
        .build());
    ActionResult actionResult = ActionResult.getDefaultInstance();
    instance.putActionResult(actionKey, actionResult);
    UpdateActionResultRequest request = reference.get();
    assertThat(request.getInstanceName()).isEqualTo(instanceName);
    assertThat(request.getActionDigest()).isEqualTo(actionKey.getDigest());
    assertThat(request.getActionResult()).isEqualTo(actionResult);
  }

  @Test
  public void findMissingBlobsCallsFindMissingBlobs() {
    AtomicReference<FindMissingBlobsRequest> reference = new AtomicReference<>();
    serviceRegistry.addService(
        new ContentAddressableStorageImplBase() {
          @Override
          public void findMissingBlobs(FindMissingBlobsRequest request, StreamObserver<FindMissingBlobsResponse> responseObserver) {
            reference.set(request);
            responseObserver.onNext(FindMissingBlobsResponse.getDefaultInstance());
            responseObserver.onCompleted();
          }
        });
    String instanceName = "findMissingBlobs-test";
    Instance instance = new StubInstance(
        instanceName,
        DIGEST_UTIL,
        InProcessChannelBuilder.forName(fakeServerName).directExecutor().build(),
        /* uploader=*/ null,
        NO_RETRIES,
        /* retryScheduler=*/ null);
    Iterable<Digest> digests = ImmutableList.of(
        Digest.newBuilder().setHash("present").setSizeBytes(1).build());
    assertThat(instance.findMissingBlobs(digests)).isEmpty();
  }

  @Test
  public void putAllBlobsUploadsBlobs() throws IOException, InterruptedException {
    String instanceName = "putAllBlobs-test";
    ByteStreamUploader uploader = mock(ByteStreamUploader.class);
    Instance instance = new StubInstance(
        instanceName,
        DIGEST_UTIL,
        /* channel=*/ null,
        uploader,
        NO_RETRIES,
        /* retryScheduler=*/ null);
    ByteString first = ByteString.copyFromUtf8("first");
    ByteString last = ByteString.copyFromUtf8("last");
    ImmutableList<ByteString> blobs = ImmutableList.of(first, last);
    ImmutableList<Digest> digests = ImmutableList.of(
        DIGEST_UTIL.compute(first),
        DIGEST_UTIL.compute(last));
    assertThat(instance.putAllBlobs(blobs)).isEqualTo(digests);
    verify(uploader, times(1)).uploadBlobs(any(Map.class));
  }

  @Test
  public void outputStreamWrites() throws Exception {
    String resourceName = "output-stream-test";
    SettableFuture<ByteString> finishedContent = SettableFuture.create();
    serviceRegistry.addService(
        new ByteStreamServiceWriter(resourceName, finishedContent));
    String instanceName = "outputStream-test";
    Instance instance = new StubInstance(
        instanceName,
        DIGEST_UTIL,
        InProcessChannelBuilder.forName(fakeServerName).directExecutor().build(),
        /* uploader=*/ null,
        NO_RETRIES,
        /* retryScheduler=*/ null);
    ByteString content = ByteString.copyFromUtf8("test-content");
    try (OutputStream out = instance.getOperationStreamWrite(resourceName).getOutput()) {
      out.write(content.toByteArray());
    }
    assertThat(finishedContent.get(1, TimeUnit.SECONDS)).isEqualTo(content);
  }

  @Test
  public void completedWriteBeforeCloseThrowsOnNextInteraction() throws IOException {
    String resourceName = "early-completed-output-stream-test";
    AtomicReference<ByteString> writtenContent = new AtomicReference<>();
    serviceRegistry.addService(
        new ByteStreamImplBase() {
          boolean completed = false;
          int writtenBytes = 0;

          @Override
          public StreamObserver<WriteRequest> write(StreamObserver<WriteResponse> responseObserver) {
            return new StreamObserver<WriteRequest>() {

              @Override
              public void onNext(WriteRequest request) {
                if (!completed) {
                  writtenBytes = request.getData().size();
                  responseObserver.onNext(WriteResponse.newBuilder()
                      .setCommittedSize(writtenBytes)
                      .build());
                  responseObserver.onCompleted();
                  completed = true;
                }
              }

              @Override
              public void onError(Throwable t) {
                t.printStackTrace();
              }

              @Override
              public void onCompleted() {
              }
            };
          }

          @Override
          public void queryWriteStatus(
              QueryWriteStatusRequest request,
              StreamObserver<QueryWriteStatusResponse> responseObserver) {
            if (request.getResourceName().equals(resourceName)) {
              responseObserver.onNext(QueryWriteStatusResponse.newBuilder()
                  .setCommittedSize(writtenBytes)
                  .setComplete(completed)
                  .build());
              responseObserver.onCompleted();
            } else {
              responseObserver.onError(Status.NOT_FOUND.asException());
            }
          }
        });
    String instanceName = "early-completed-outputStream-test";
    Instance instance = new StubInstance(
        instanceName,
        DIGEST_UTIL,
        InProcessChannelBuilder.forName(fakeServerName).directExecutor().build(),
        /* uploader=*/ null,
        NO_RETRIES,
        /* retryScheduler=*/ null);
    ByteString content = ByteString.copyFromUtf8("test-content");
    boolean writeThrewException = false;
    try (OutputStream out = instance.getOperationStreamWrite(resourceName).getOutput()) {
      out.write(content.toByteArray());
      try {
        out.write(content.toByteArray()); // should throw
      } catch (Exception e) {
        writeThrewException = true;
      }
    }
    assertThat(writeThrewException).isTrue();
  }
}
