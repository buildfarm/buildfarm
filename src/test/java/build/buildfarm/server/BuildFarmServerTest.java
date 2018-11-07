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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.fail;
import static build.buildfarm.instance.AbstractServerInstance.VIOLATION_TYPE_INVALID;

import build.buildfarm.common.DigestUtil;
import build.buildfarm.instance.stub.Chunker;
import build.buildfarm.instance.stub.ByteStreamUploader;
import build.buildfarm.instance.stub.Retrier;
import build.buildfarm.instance.stub.RetryException;
import build.buildfarm.server.BuildFarmServer;
import build.buildfarm.v1test.BuildFarmServerConfig;
import build.buildfarm.v1test.InstanceConfig.HashFunction;
import build.buildfarm.v1test.MemoryInstanceConfig;
import build.buildfarm.v1test.OperationQueueGrpc;
import build.buildfarm.v1test.TakeOperationRequest;
import build.buildfarm.v1test.PollOperationRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.devtools.remoteexecution.v1test.Action;
import com.google.devtools.remoteexecution.v1test.ActionCacheGrpc;
import com.google.devtools.remoteexecution.v1test.BatchUpdateBlobsRequest;
import com.google.devtools.remoteexecution.v1test.BatchUpdateBlobsResponse;
import com.google.devtools.remoteexecution.v1test.Command;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.Directory;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata.Stage;
import com.google.devtools.remoteexecution.v1test.ExecuteRequest;
import com.google.devtools.remoteexecution.v1test.ExecutionGrpc;
import com.google.devtools.remoteexecution.v1test.FindMissingBlobsRequest;
import com.google.devtools.remoteexecution.v1test.FindMissingBlobsResponse;
import com.google.devtools.remoteexecution.v1test.GetActionResultRequest;
import com.google.devtools.remoteexecution.v1test.UpdateBlobRequest;
import com.google.devtools.remoteexecution.v1test.ContentAddressableStorageGrpc;
import com.google.longrunning.CancelOperationRequest;
import com.google.longrunning.GetOperationRequest;
import com.google.longrunning.ListOperationsRequest;
import com.google.longrunning.ListOperationsResponse;
import com.google.longrunning.Operation;
import com.google.longrunning.OperationsGrpc;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
import com.google.rpc.PreconditionFailure;
import com.google.rpc.PreconditionFailure.Violation;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.util.Collections;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BuildFarmServerTest {
  private BuildFarmServer server;
  private ManagedChannel inProcessChannel;
  private MemoryInstanceConfig memoryInstanceConfig;

  @Before
  public void setUp() throws Exception {
    String uniqueServerName = "in-process server for " + getClass();

    memoryInstanceConfig = MemoryInstanceConfig.newBuilder()
        .setListOperationsDefaultPageSize(1024)
        .setListOperationsMaxPageSize(16384)
        .setTreeDefaultPageSize(1024)
        .setTreeMaxPageSize(16384)
        .setOperationPollTimeout(Duration.newBuilder()
            .setSeconds(10)
            .setNanos(0))
        .setOperationCompletedDelay(Duration.newBuilder()
            .setSeconds(10)
            .setNanos(0))
        .setCasMaxSizeBytes(640 * 1024)
        .setDefaultActionTimeout(Duration.newBuilder()
            .setSeconds(600)
            .setNanos(0))
        .setMaximumActionTimeout(Duration.newBuilder()
            .setSeconds(3600)
            .setNanos(0))
        .build();

    BuildFarmServerConfig.Builder configBuilder =
        BuildFarmServerConfig.newBuilder().setPort(0);
    configBuilder.addInstancesBuilder()
        .setName("memory")
        .setHashFunction(HashFunction.SHA256)
        .setMemoryInstanceConfig(memoryInstanceConfig);

    server = new BuildFarmServer(
        InProcessServerBuilder.forName(uniqueServerName).directExecutor(),
        configBuilder.build());
    server.start();
    inProcessChannel = InProcessChannelBuilder.forName(uniqueServerName)
        .directExecutor().build();
  }

  @After
  public void tearDown() throws Exception {
    inProcessChannel.shutdownNow();
    server.stop();
  }

  @Test
  public void findMissingBlobs() {
    DigestUtil digestUtil = new DigestUtil(DigestUtil.HashFunction.SHA256);
    ByteString content = ByteString.copyFromUtf8("Hello, World!");
    Iterable<Digest> digests =
        Collections.singleton(digestUtil.compute(content));
    FindMissingBlobsRequest request = FindMissingBlobsRequest.newBuilder()
        .setInstanceName("memory")
        .addAllBlobDigests(digests)
        .build();
    ContentAddressableStorageGrpc.ContentAddressableStorageBlockingStub stub =
        ContentAddressableStorageGrpc.newBlockingStub(inProcessChannel);

    FindMissingBlobsResponse response = stub.findMissingBlobs(request);

    assertThat(response.getMissingBlobDigestsList())
        .containsExactlyElementsIn(digests);
  }

  @Test
  public void batchUpdateBlobs() {
    DigestUtil digestUtil = new DigestUtil(DigestUtil.HashFunction.SHA256);
    ByteString content = ByteString.copyFromUtf8("Hello, World!");
    Digest digest = digestUtil.compute(content);
    BatchUpdateBlobsRequest request = BatchUpdateBlobsRequest.newBuilder()
        .setInstanceName("memory")
        .addRequests(UpdateBlobRequest.newBuilder()
            .setContentDigest(digest)
            .setData(content)
            .build())
        .build();
    ContentAddressableStorageGrpc.ContentAddressableStorageBlockingStub stub =
        ContentAddressableStorageGrpc.newBlockingStub(inProcessChannel);

    BatchUpdateBlobsResponse response = stub.batchUpdateBlobs(request);

    BatchUpdateBlobsResponse.Response expected = BatchUpdateBlobsResponse.Response.newBuilder()
        .setBlobDigest(digest)
        .setStatus(com.google.rpc.Status.newBuilder()
            .setCode(Code.OK.getNumber())
            .build())
        .build();
    assertThat(response.getResponsesList())
        .containsExactlyElementsIn(Collections.singleton(expected));
  }

  @Test
  public void listOperations() {
    ListOperationsRequest request = ListOperationsRequest.newBuilder()
        .setName("memory/operations")
        .setPageSize(1024)
        .build();

    OperationsGrpc.OperationsBlockingStub stub =
        OperationsGrpc.newBlockingStub(inProcessChannel);

    ListOperationsResponse response = stub.listOperations(request);

    assertThat(response.getOperationsList()).isEmpty();
  }

  @Test
  public void canceledOperationIsNoLongerOutstanding() throws RetryException, InterruptedException {
    Operation operation = executeAction(createSimpleAction());

    // should appear in outstanding list
    ListOperationsRequest listRequest = ListOperationsRequest.newBuilder()
        .setName("memory/operations")
        .setPageSize(1024)
        .build();

    OperationsGrpc.OperationsBlockingStub operationsStub =
        OperationsGrpc.newBlockingStub(inProcessChannel);

    ListOperationsResponse listResponse = operationsStub.listOperations(listRequest);

    assertThat(Iterables.transform(listResponse.getOperationsList(), o -> o.getName())).containsExactly(operation.getName());

    CancelOperationRequest cancelRequest = CancelOperationRequest.newBuilder()
        .setName(operation.getName())
        .build();

    operationsStub.cancelOperation(cancelRequest);

    // should now be gone
    listResponse = operationsStub.listOperations(listRequest);

    assertThat(listResponse.getOperationsList()).isEmpty();
  }

  @Test
  public void canceledOperationHasCancelledState()
      throws RetryException, InterruptedException, InvalidProtocolBufferException {
    Operation operation = executeAction(createSimpleAction());

    OperationsGrpc.OperationsBlockingStub operationsStub =
        OperationsGrpc.newBlockingStub(inProcessChannel);

    // should be available with cancelled state
    Operation preCancelOperation = getOperation(operation.getName());

    assertThat(preCancelOperation.getDone()).isFalse();

    CancelOperationRequest cancelRequest = CancelOperationRequest.newBuilder()
        .setName(operation.getName())
        .build();

    operationsStub.cancelOperation(cancelRequest);

    Operation cancelledOperation = getOperation(operation.getName());

    assertThat(cancelledOperation.getDone()).isTrue();
    assertThat(cancelledOperation.getResultCase()).isEqualTo(Operation.ResultCase.ERROR);
    assertThat(cancelledOperation.getError().getCode()).isEqualTo(Code.CANCELLED.getNumber());
  }

  @Test
  public void cancellingExecutingOperationFailsPoll()
      throws RetryException, InterruptedException, InvalidProtocolBufferException {
    Operation operation = executeAction(createSimpleAction());

    // take our operation from the queue
    TakeOperationRequest takeRequest = TakeOperationRequest.newBuilder()
        .setInstanceName("memory")
        .build();

    OperationQueueGrpc.OperationQueueBlockingStub operationQueueStub =
        OperationQueueGrpc.newBlockingStub(inProcessChannel);

    Operation givenOperation = operationQueueStub.take(takeRequest);

    assertThat(givenOperation.getName()).isEqualTo(operation.getName());

    // move the operation into EXECUTING stage
    ExecuteOperationMetadata executingMetadata =
      givenOperation.getMetadata().unpack(ExecuteOperationMetadata.class);

    executingMetadata = executingMetadata.toBuilder()
        .setStage(ExecuteOperationMetadata.Stage.EXECUTING)
        .build();

    Operation executingOperation = givenOperation.toBuilder()
        .setMetadata(Any.pack(executingMetadata))
        .build();

    assertThat(operationQueueStub.put(executingOperation).getCode()).isEqualTo(Code.OK.getNumber());

    // poll should succeed
    assertThat(operationQueueStub
        .poll(PollOperationRequest.newBuilder()
            .setOperationName(executingOperation.getName())
            .setStage(ExecuteOperationMetadata.Stage.EXECUTING)
            .build())
        .getCode()).isEqualTo(Code.OK.getNumber());

    OperationsGrpc.OperationsBlockingStub operationsStub =
        OperationsGrpc.newBlockingStub(inProcessChannel);

    operationsStub.cancelOperation(CancelOperationRequest.newBuilder()
        .setName(executingOperation.getName())
        .build());

    // poll should fail
    assertThat(operationQueueStub
        .poll(PollOperationRequest.newBuilder()
            .setStage(ExecuteOperationMetadata.Stage.EXECUTING)
            .setOperationName(executingOperation.getName())
            .build())
        .getCode()).isEqualTo(Code.UNAVAILABLE.getNumber());
  }

  private Operation getOperation(String name) {
    GetOperationRequest getRequest = GetOperationRequest.newBuilder()
        .setName(name)
        .build();

    OperationsGrpc.OperationsBlockingStub operationsStub =
        OperationsGrpc.newBlockingStub(inProcessChannel);

    return operationsStub.getOperation(getRequest);
  }

  @Test
  public void actionWithExcessiveTimeoutFailsValidation()
      throws RetryException, InterruptedException, InvalidProtocolBufferException {
    Action actionWithExcessiveTimeout = createSimpleAction().toBuilder()
        .setTimeout(Duration.newBuilder().setSeconds(9000))
        .build();

    Operation operation = executeAction(actionWithExcessiveTimeout);

    Operation failedOperation = getOperation(operation.getName());
    assertThat(failedOperation.getDone()).isTrue();
    assertThat(
        failedOperation
            .getMetadata()
            .unpack(ExecuteOperationMetadata.class).getStage())
        .isEqualTo(Stage.COMPLETED);
    assertThat(failedOperation.getError().getCode())
        .isEqualTo(Code.FAILED_PRECONDITION.getNumber());
    assertThat(failedOperation.getError().getDetailsCount()).isEqualTo(1);
    PreconditionFailure preconditionFailure = failedOperation
        .getError().getDetailsList().get(0).unpack(PreconditionFailure.class);
    assertThat(preconditionFailure.getViolationsCount()).isEqualTo(1);
    Violation violation = preconditionFailure.getViolationsList().get(0);
    assertThat(violation.getType()).isEqualTo(VIOLATION_TYPE_INVALID);
  }

  private Action createSimpleAction() throws RetryException, InterruptedException {
    DigestUtil digestUtil = new DigestUtil(DigestUtil.HashFunction.SHA256);
    Command command = Command.newBuilder()
        .addArguments("echo")
        .build();
    Digest commandBlobDigest = digestUtil.compute(command);
    Directory root = Directory.getDefaultInstance();
    Digest rootBlobDigest = digestUtil.compute(root);
    Action action = Action.newBuilder()
        .setCommandDigest(commandBlobDigest)
        .setInputRootDigest(rootBlobDigest)
        .build();
    ByteStreamUploader uploader = new ByteStreamUploader("memory", inProcessChannel, null, 60, Retrier.NO_RETRIES, null);

    uploader.uploadBlobs(ImmutableList.of(
        new Chunker(command.toByteString(), commandBlobDigest)));
    return action;
  }

  private Operation executeAction(Action action) {
    ExecuteRequest executeRequest = ExecuteRequest.newBuilder()
        .setInstanceName("memory")
        .setAction(action)
        .setSkipCacheLookup(true)
        .build();

    ExecutionGrpc.ExecutionBlockingStub executeStub =
        ExecutionGrpc.newBlockingStub(inProcessChannel);

    return executeStub.execute(executeRequest);
  }

  @Test
  public void actionNotCached() {
    DigestUtil digestUtil = new DigestUtil(DigestUtil.HashFunction.SHA256);
    GetActionResultRequest request = GetActionResultRequest.newBuilder()
        .setInstanceName("memory")
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
}
