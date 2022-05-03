// Copyright 2017 The Bazel Authors. All rights reserved.
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

package build.buildfarm.server;

import static build.bazel.remote.execution.v2.ExecutionStage.Value.COMPLETED;
import static build.bazel.remote.execution.v2.ExecutionStage.Value.EXECUTING;
import static build.buildfarm.common.Errors.VIOLATION_TYPE_INVALID;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionCacheGrpc;
import build.bazel.remote.execution.v2.BatchReadBlobsRequest;
import build.bazel.remote.execution.v2.BatchReadBlobsResponse;
import build.bazel.remote.execution.v2.BatchUpdateBlobsRequest;
import build.bazel.remote.execution.v2.BatchUpdateBlobsRequest.Request;
import build.bazel.remote.execution.v2.BatchUpdateBlobsResponse;
import build.bazel.remote.execution.v2.BatchUpdateBlobsResponse.Response;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecuteRequest;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.bazel.remote.execution.v2.ExecutionGrpc;
import build.bazel.remote.execution.v2.FindMissingBlobsRequest;
import build.bazel.remote.execution.v2.FindMissingBlobsResponse;
import build.bazel.remote.execution.v2.GetActionResultRequest;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.grpc.Retrier;
import build.buildfarm.instance.stub.ByteStreamUploader;
import build.buildfarm.instance.stub.Chunker;
import build.buildfarm.v1test.ActionCacheConfig;
import build.buildfarm.v1test.BuildFarmServerConfig;
import build.buildfarm.v1test.ContentAddressableStorageConfig;
import build.buildfarm.v1test.DelegateCASConfig;
import build.buildfarm.v1test.GrpcPrometheusMetrics;
import build.buildfarm.v1test.MemoryCASConfig;
import build.buildfarm.v1test.MemoryInstanceConfig;
import build.buildfarm.v1test.OperationQueueGrpc;
import build.buildfarm.v1test.PollOperationRequest;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.TakeOperationRequest;
import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.hash.HashCode;
import com.google.longrunning.CancelOperationRequest;
import com.google.longrunning.GetOperationRequest;
import com.google.longrunning.ListOperationsRequest;
import com.google.longrunning.ListOperationsResponse;
import com.google.longrunning.Operation;
import com.google.longrunning.OperationsGrpc;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;
import com.google.rpc.Code;
import com.google.rpc.PreconditionFailure;
import com.google.rpc.PreconditionFailure.Violation;
import io.grpc.ManagedChannel;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Collections;
import java.util.UUID;
import me.dinowernli.grpc.prometheus.MonitoringServerInterceptor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BuildFarmServerTest {
  private static final String INSTANCE_NAME = "memory";

  private BuildFarmServer server;
  private ManagedChannel inProcessChannel;

  @Before
  public void setUp() throws Exception {
    String uniqueServerName = "in-process server for " + getClass();

    MemoryInstanceConfig memoryInstanceConfig =
        MemoryInstanceConfig.newBuilder()
            .setListOperationsDefaultPageSize(1024)
            .setListOperationsMaxPageSize(16384)
            .setTreeDefaultPageSize(1024)
            .setTreeMaxPageSize(16384)
            .setOperationPollTimeout(Durations.fromSeconds(10))
            .setOperationCompletedDelay(Durations.fromSeconds(10))
            .setCasConfig(
                ContentAddressableStorageConfig.newBuilder()
                    .setMemory(MemoryCASConfig.newBuilder().setMaxSizeBytes(640 * 1024)))
            .setActionCacheConfig(
                ActionCacheConfig.newBuilder()
                    .setDelegateCas(DelegateCASConfig.getDefaultInstance())
                    .build())
            .setDefaultActionTimeout(Durations.fromSeconds(600))
            .setMaximumActionTimeout(Durations.fromSeconds(3600))
            .build();

    BuildFarmServerConfig.Builder configBuilder = BuildFarmServerConfig.newBuilder().setPort(0);
    configBuilder
        .getInstanceBuilder()
        .setName(INSTANCE_NAME)
        .setDigestFunction(DigestFunction.Value.SHA256)
        .setMemoryInstanceConfig(memoryInstanceConfig);

    server =
        new BuildFarmServer(
            "test",
            InProcessServerBuilder.forName(uniqueServerName).directExecutor(),
            configBuilder.build());
    server.start("startTime/test:0000", 0);
    inProcessChannel = InProcessChannelBuilder.forName(uniqueServerName).directExecutor().build();
  }

  @After
  public void tearDown() throws Exception {
    inProcessChannel.shutdownNow();
    server.stop();
  }

  ByteString getBlob(Digest digest) {
    ContentAddressableStorageGrpc.ContentAddressableStorageBlockingStub stub =
        ContentAddressableStorageGrpc.newBlockingStub(inProcessChannel);
    BatchReadBlobsResponse batchResponse =
        stub.batchReadBlobs(
            BatchReadBlobsRequest.newBuilder()
                .setInstanceName(INSTANCE_NAME)
                .addDigests(digest)
                .build());
    BatchReadBlobsResponse.Response response = batchResponse.getResponsesList().get(0);
    com.google.rpc.Status status = response.getStatus();
    if (Code.forNumber(status.getCode()) == Code.NOT_FOUND) {
      return null;
    }
    if (Code.forNumber(status.getCode()) != Code.OK) {
      throw StatusProto.toStatusRuntimeException(status);
    }
    return response.getData();
  }

  @Test
  public void findMissingBlobs() {
    DigestUtil digestUtil = new DigestUtil(HashFunction.SHA256);
    ByteString content = ByteString.copyFromUtf8("Hello, World!");
    Iterable<Digest> digests = Collections.singleton(digestUtil.compute(content));
    FindMissingBlobsRequest request =
        FindMissingBlobsRequest.newBuilder()
            .setInstanceName(INSTANCE_NAME)
            .addAllBlobDigests(digests)
            .build();
    ContentAddressableStorageGrpc.ContentAddressableStorageBlockingStub stub =
        ContentAddressableStorageGrpc.newBlockingStub(inProcessChannel);

    FindMissingBlobsResponse response = stub.findMissingBlobs(request);

    assertThat(response.getMissingBlobDigestsList()).containsExactlyElementsIn(digests);
  }

  @Test
  public void batchUpdateBlobs() {
    DigestUtil digestUtil = new DigestUtil(HashFunction.SHA256);
    ByteString content = ByteString.copyFromUtf8("Hello, World!");
    Digest digest = digestUtil.compute(content);
    BatchUpdateBlobsRequest request =
        BatchUpdateBlobsRequest.newBuilder()
            .setInstanceName(INSTANCE_NAME)
            .addRequests(Request.newBuilder().setDigest(digest).setData(content).build())
            .build();
    ContentAddressableStorageGrpc.ContentAddressableStorageBlockingStub stub =
        ContentAddressableStorageGrpc.newBlockingStub(inProcessChannel);

    BatchUpdateBlobsResponse response = stub.batchUpdateBlobs(request);

    Response expected =
        Response.newBuilder()
            .setDigest(digest)
            .setStatus(com.google.rpc.Status.newBuilder().setCode(Code.OK.getNumber()).build())
            .build();
    assertThat(response.getResponsesList())
        .containsExactlyElementsIn(Collections.singleton(expected));
  }

  @Test
  public void listOperations() {
    ListOperationsRequest request =
        ListOperationsRequest.newBuilder()
            .setName(INSTANCE_NAME + "/operations")
            .setPageSize(1024)
            .build();

    OperationsGrpc.OperationsBlockingStub stub = OperationsGrpc.newBlockingStub(inProcessChannel);

    ListOperationsResponse response = stub.listOperations(request);

    assertThat(response.getOperationsList()).isEmpty();
  }

  @Test
  public void canceledOperationIsNoLongerOutstanding() throws IOException, InterruptedException {
    Operation operation = executeAction(createSimpleAction());

    // should appear in outstanding list
    ListOperationsRequest listRequest =
        ListOperationsRequest.newBuilder()
            .setName(INSTANCE_NAME + "/operations")
            .setPageSize(1024)
            .build();

    OperationsGrpc.OperationsBlockingStub operationsStub =
        OperationsGrpc.newBlockingStub(inProcessChannel);

    ListOperationsResponse listResponse = operationsStub.listOperations(listRequest);

    assertThat(Iterables.transform(listResponse.getOperationsList(), Operation::getName))
        .containsExactly(operation.getName());

    CancelOperationRequest cancelRequest =
        CancelOperationRequest.newBuilder().setName(operation.getName()).build();

    operationsStub.cancelOperation(cancelRequest);

    // should now be gone
    listResponse = operationsStub.listOperations(listRequest);

    assertThat(listResponse.getOperationsList()).isEmpty();
  }

  @Test
  public void cancelledOperationHasCancelledState() throws IOException, InterruptedException {
    Operation operation = executeAction(createSimpleAction());

    OperationsGrpc.OperationsBlockingStub operationsStub =
        OperationsGrpc.newBlockingStub(inProcessChannel);

    // should be available with cancelled state
    Operation preCancelOperation = getOperation(operation.getName());

    assertThat(preCancelOperation.getDone()).isFalse();

    CancelOperationRequest cancelRequest =
        CancelOperationRequest.newBuilder().setName(operation.getName()).build();

    operationsStub.cancelOperation(cancelRequest);

    Operation cancelledOperation = getOperation(operation.getName());

    assertThat(cancelledOperation.getDone()).isTrue();
    assertThat(cancelledOperation.getResultCase()).isEqualTo(Operation.ResultCase.RESPONSE);
    ExecuteResponse executeResponse =
        cancelledOperation.getResponse().unpack(ExecuteResponse.class);
    assertThat(executeResponse.getStatus().getCode()).isEqualTo(Code.CANCELLED.getNumber());
  }

  @Test
  public void cancellingExecutingOperationFailsPoll() throws IOException, InterruptedException {
    Operation operation = executeAction(createSimpleAction());

    // take our operation from the queue
    TakeOperationRequest takeRequest =
        TakeOperationRequest.newBuilder().setInstanceName(INSTANCE_NAME).build();

    OperationQueueGrpc.OperationQueueBlockingStub operationQueueStub =
        OperationQueueGrpc.newBlockingStub(inProcessChannel);

    QueueEntry givenEntry = operationQueueStub.take(takeRequest);
    String givenOperationName = givenEntry.getExecuteEntry().getOperationName();

    assertThat(givenOperationName).isEqualTo(operation.getName());

    Operation givenOperation = getOperation(givenOperationName);

    // move the operation into EXECUTING stage
    ExecuteOperationMetadata executingMetadata =
        givenOperation.getMetadata().unpack(ExecuteOperationMetadata.class);

    executingMetadata = executingMetadata.toBuilder().setStage(EXECUTING).build();

    Operation executingOperation =
        givenOperation.toBuilder().setMetadata(Any.pack(executingMetadata)).build();

    assertThat(operationQueueStub.put(executingOperation).getCode()).isEqualTo(Code.OK.getNumber());

    // poll should succeed
    assertThat(
            operationQueueStub
                .poll(
                    PollOperationRequest.newBuilder()
                        .setOperationName(executingOperation.getName())
                        .setStage(EXECUTING)
                        .build())
                .getCode())
        .isEqualTo(Code.OK.getNumber());

    OperationsGrpc.OperationsBlockingStub operationsStub =
        OperationsGrpc.newBlockingStub(inProcessChannel);

    operationsStub.cancelOperation(
        CancelOperationRequest.newBuilder().setName(executingOperation.getName()).build());

    // poll should fail
    assertThat(
            operationQueueStub
                .poll(
                    PollOperationRequest.newBuilder()
                        .setStage(EXECUTING)
                        .setOperationName(executingOperation.getName())
                        .build())
                .getCode())
        .isNotEqualTo(Code.OK.getNumber());
  }

  private Operation getOperation(String name) {
    GetOperationRequest getRequest = GetOperationRequest.newBuilder().setName(name).build();

    OperationsGrpc.OperationsBlockingStub operationsStub =
        OperationsGrpc.newBlockingStub(inProcessChannel);

    return operationsStub.getOperation(getRequest);
  }

  @Test
  public void actionWithExcessiveTimeoutFailsValidation() throws IOException, InterruptedException {
    Digest actionDigestWithExcessiveTimeout =
        createAction(Action.newBuilder().setTimeout(Duration.newBuilder().setSeconds(9000)));

    Operation failedOperation = executeAction(actionDigestWithExcessiveTimeout);
    assertThat(failedOperation.getDone()).isTrue();
    assertThat(failedOperation.getMetadata().unpack(ExecuteOperationMetadata.class).getStage())
        .isEqualTo(COMPLETED);
    ExecuteResponse executeResponse = failedOperation.getResponse().unpack(ExecuteResponse.class);
    com.google.rpc.Status status = executeResponse.getStatus();
    assertThat(status.getCode()).isEqualTo(Code.FAILED_PRECONDITION.getNumber());
    assertThat(status.getDetailsCount()).isEqualTo(1);
    PreconditionFailure preconditionFailure =
        status.getDetailsList().get(0).unpack(PreconditionFailure.class);
    assertThat(preconditionFailure.getViolationsCount()).isEqualTo(1);
    Violation violation = preconditionFailure.getViolationsList().get(0);
    assertThat(violation.getType()).isEqualTo(VIOLATION_TYPE_INVALID);
  }

  private Digest createSimpleAction() throws IOException, InterruptedException {
    return createAction(Action.newBuilder());
  }

  private Digest createAction(Action.Builder actionBuilder)
      throws IOException, InterruptedException {
    DigestUtil digestUtil = new DigestUtil(HashFunction.SHA256);
    Command command = Command.newBuilder().addArguments("echo").build();
    Digest commandDigest = digestUtil.compute(command);
    Directory root = Directory.getDefaultInstance();
    Digest rootBlobDigest = digestUtil.compute(root);
    Action action =
        actionBuilder.setCommandDigest(commandDigest).setInputRootDigest(rootBlobDigest).build();
    Digest actionDigest = digestUtil.compute(action);
    ByteStreamUploader uploader =
        new ByteStreamUploader(INSTANCE_NAME, inProcessChannel, null, 60, Retrier.NO_RETRIES);

    uploader.uploadBlobs(
        ImmutableMap.of(
            HashCode.fromString(actionDigest.getHash()),
            Chunker.builder().setInput(action.toByteString()).build(),
            HashCode.fromString(commandDigest.getHash()),
            Chunker.builder().setInput(command.toByteString()).build()));
    return actionDigest;
  }

  private Operation executeAction(Digest actionDigest) {
    ExecuteRequest executeRequest =
        ExecuteRequest.newBuilder()
            .setInstanceName(INSTANCE_NAME)
            .setActionDigest(actionDigest)
            .setSkipCacheLookup(true)
            .build();

    ExecutionGrpc.ExecutionBlockingStub executeStub =
        ExecutionGrpc.newBlockingStub(inProcessChannel);

    return executeStub.execute(executeRequest).next();
  }

  @Test
  public void actionNotCached() {
    DigestUtil digestUtil = new DigestUtil(HashFunction.SHA256);
    GetActionResultRequest request =
        GetActionResultRequest.newBuilder()
            .setInstanceName(INSTANCE_NAME)
            .setActionDigest(digestUtil.empty())
            .build();

    ActionCacheGrpc.ActionCacheBlockingStub actionCacheStub =
        ActionCacheGrpc.newBlockingStub(inProcessChannel);

    try {
      actionCacheStub.getActionResult(request);
      fail("expected exception");
    } catch (StatusRuntimeException e) {
      assertThat(e.getStatus().getCode()).isEqualTo(io.grpc.Status.Code.NOT_FOUND);
    }
  }

  @Test
  public void progressiveUploadCompletes() throws Exception {
    DigestUtil digestUtil = new DigestUtil(HashFunction.SHA256);
    ByteString content = ByteString.copyFromUtf8("Hello, World!");
    Digest digest = digestUtil.compute(content);
    HashCode hash = HashCode.fromString(digest.getHash());
    UUID uuid = UUID.randomUUID();
    String resourceName =
        ByteStreamUploader.uploadResourceName(INSTANCE_NAME, uuid, hash, content.size());

    assertThat(getBlob(digest)).isNull();

    FutureWriteResponseObserver futureResponder = new FutureWriteResponseObserver();
    StreamObserver<WriteRequest> requestObserver =
        ByteStreamGrpc.newStub(inProcessChannel).write(futureResponder);
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
    requestObserver = ByteStreamGrpc.newStub(inProcessChannel).write(futureResponder);
    requestObserver.onNext(
        WriteRequest.newBuilder()
            .setWriteOffset(6)
            .setResourceName(resourceName)
            .setData(content.substring(6))
            .setFinishWrite(true)
            .build());
    requestObserver.onCompleted();
    assertThat(futureResponder.get())
        .isEqualTo(WriteResponse.newBuilder().setCommittedSize(content.size()).build());

    assertThat(getBlob(digest)).isEqualTo(content);
  }

  @Test
  public void grpcMetricsOffByDefault() {
    // ARRANGE
    ServerBuilder serverBuilder = mock(ServerBuilder.class);
    BuildFarmServerConfig config = BuildFarmServerConfig.newBuilder().build();

    // ACT
    BuildFarmServer.handleGrpcMetricIntercepts(serverBuilder, config);

    // ASSERT
    verify(serverBuilder, times(0)).intercept(any(MonitoringServerInterceptor.class));
  }

  @Test
  public void grpcMetricsEnabled() {
    // ARRANGE
    ServerBuilder serverBuilder = mock(ServerBuilder.class);
    GrpcPrometheusMetrics metricsConfig =
        GrpcPrometheusMetrics.newBuilder().setEnabled(true).build();
    BuildFarmServerConfig config =
        BuildFarmServerConfig.newBuilder().setGrpcMetrics(metricsConfig).build();

    // ACT
    BuildFarmServer.handleGrpcMetricIntercepts(serverBuilder, config);

    // ASSERT
    verify(serverBuilder, times(1)).intercept(any(MonitoringServerInterceptor.class));
  }
}
