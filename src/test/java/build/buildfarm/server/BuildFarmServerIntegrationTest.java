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
import static build.buildfarm.common.config.Cas.TYPE.MEMORY;
import static build.buildfarm.common.config.Server.INSTANCE_TYPE.SHARD;
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
import build.buildfarm.common.config.BuildfarmConfigs;
import build.buildfarm.common.config.GrpcMetrics;
import build.buildfarm.common.config.Queue;
import build.buildfarm.common.grpc.Retrier;
import build.buildfarm.instance.stub.ByteStreamUploader;
import build.buildfarm.instance.stub.Chunker;
import build.buildfarm.v1test.Digest;
import build.buildfarm.v1test.OperationQueueGrpc;
import build.buildfarm.v1test.PollOperationRequest;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.TakeOperationRequest;
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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;
import me.dinowernli.grpc.prometheus.MonitoringServerInterceptor;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BuildFarmServerIntegrationTest {
  private static final String INSTANCE_NAME = "shard";

  private BuildFarmServer server;
  private ManagedChannel inProcessChannel;

  private BuildfarmConfigs configs = BuildfarmConfigs.getInstance();

  ByteString content = ByteString.copyFromUtf8("Hello, World! " + UUID.randomUUID());

  @Before
  public void setUp() throws Exception {
    configs.getServer().setClusterId("buildfarm-test");
    configs.getServer().setInstanceType(SHARD);
    configs.getServer().setName("shard");
    configs.getWorker().setPublicName("localhost:8981");
    configs.getBackplane().setRedisUri("redis://localhost:6379");
    Queue queue = new Queue();
    queue.setName("test");
    queue.setProperties(new ArrayList<>());
    Queue[] queues = new Queue[1];
    queues[0] = queue;
    configs.getBackplane().setQueues(queues);
    configs.getWorker().getStorages().get(0).setType(MEMORY);
    String uniqueServerName = "in-process server for " + getClass();
    server = new BuildFarmServer();
    server.start(
        InProcessServerBuilder.forName(uniqueServerName).directExecutor(), "startTime/test:0000");
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
                .addDigests(DigestUtil.toDigest(digest))
                .setDigestFunction(digest.getDigestFunction())
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
    Iterable<build.bazel.remote.execution.v2.Digest> digests =
        Collections.singleton(DigestUtil.toDigest(digestUtil.compute(content)));
    FindMissingBlobsRequest request =
        FindMissingBlobsRequest.newBuilder()
            .setInstanceName(INSTANCE_NAME)
            .addAllBlobDigests(digests)
            .setDigestFunction(digestUtil.getDigestFunction())
            .build();
    ContentAddressableStorageGrpc.ContentAddressableStorageBlockingStub stub =
        ContentAddressableStorageGrpc.newBlockingStub(inProcessChannel);

    FindMissingBlobsResponse response = stub.findMissingBlobs(request);

    assertThat(response.getMissingBlobDigestsList()).containsExactlyElementsIn(digests);
  }

  @Test
  public void batchUpdateBlobs() {
    DigestUtil digestUtil = new DigestUtil(HashFunction.SHA256);
    Digest digest = digestUtil.compute(content);
    BatchUpdateBlobsRequest request =
        BatchUpdateBlobsRequest.newBuilder()
            .setInstanceName(INSTANCE_NAME)
            .addRequests(
                Request.newBuilder()
                    .setDigest(DigestUtil.toDigest(digest))
                    .setData(content)
                    .build())
            .setDigestFunction(digest.getDigestFunction())
            .build();
    ContentAddressableStorageGrpc.ContentAddressableStorageBlockingStub stub =
        ContentAddressableStorageGrpc.newBlockingStub(inProcessChannel);

    BatchUpdateBlobsResponse response = stub.batchUpdateBlobs(request);

    Response expected =
        Response.newBuilder()
            .setDigest(DigestUtil.toDigest(digest))
            .setStatus(com.google.rpc.Status.newBuilder().setCode(Code.OK.getNumber()).build())
            .build();
    assertThat(response.getResponsesList())
        .containsExactlyElementsIn(Collections.singleton(expected));
  }

  @Test
  public void canceledOperationIsNoLongerOutstanding() throws IOException, InterruptedException {
    Operation operation = executeAction(createSimpleAction());

    // should appear in outstanding list
    ListOperationsRequest listRequest =
        ListOperationsRequest.newBuilder().setName(INSTANCE_NAME + "/executions").build();

    OperationsGrpc.OperationsBlockingStub operationsStub =
        OperationsGrpc.newBlockingStub(inProcessChannel);

    ListOperationsResponse listResponse = operationsStub.listOperations(listRequest);

    assertThat(Iterables.transform(listResponse.getOperationsList(), Operation::getName))
        .contains(operation.getName());

    CancelOperationRequest cancelRequest =
        CancelOperationRequest.newBuilder().setName(operation.getName()).build();

    operationsStub.cancelOperation(cancelRequest);

    // should now be gone
    listResponse = operationsStub.listOperations(listRequest);

    assertThat(listResponse.getOperationsList()).doesNotContain(operation.getName());
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
  @Ignore
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
  @Ignore
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
        actionBuilder
            .setCommandDigest(DigestUtil.toDigest(commandDigest))
            .setInputRootDigest(DigestUtil.toDigest(rootBlobDigest))
            .build();
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
            .setActionDigest(DigestUtil.toDigest(actionDigest))
            .setDigestFunction(actionDigest.getDigestFunction())
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
            .setActionDigest(DigestUtil.toDigest(digestUtil.empty()))
            .setDigestFunction(digestUtil.getDigestFunction())
            .build();

    ActionCacheGrpc.ActionCacheBlockingStub actionCacheStub =
        ActionCacheGrpc.newBlockingStub(inProcessChannel);

    try {
      actionCacheStub.getActionResult(request);
      fail("expected exception");
    } catch (StatusRuntimeException e) {
      assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
    }
  }

  @Test
  public void grpcMetricsOffByDefault() {
    configs.getServer().getGrpcMetrics().setEnabled(false);
    // ARRANGE
    ServerBuilder serverBuilder = mock(ServerBuilder.class);

    // ACT
    GrpcMetrics.handleGrpcMetricIntercepts(serverBuilder, configs.getServer().getGrpcMetrics());

    // ASSERT
    verify(serverBuilder, times(0)).intercept(any(MonitoringServerInterceptor.class));
  }

  @Test
  public void grpcMetricsEnabled() {
    configs.getServer().getGrpcMetrics().setEnabled(true);
    // ARRANGE
    ServerBuilder serverBuilder = mock(ServerBuilder.class);

    // ACT
    GrpcMetrics.handleGrpcMetricIntercepts(serverBuilder, configs.getServer().getGrpcMetrics());

    // ASSERT
    verify(serverBuilder, times(1)).intercept(any(MonitoringServerInterceptor.class));
  }
}
