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

import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.instance.stub.Chunker;
import build.buildfarm.instance.stub.ByteStreamUploader;
import build.buildfarm.instance.stub.Retrier;
import build.buildfarm.instance.stub.RetryException;
import build.buildfarm.server.BuildFarmServer;
import build.buildfarm.v1test.ActionCacheConfig;
import build.buildfarm.v1test.BuildFarmServerConfig;
import build.buildfarm.v1test.ContentAddressableStorageConfig;
import build.buildfarm.v1test.DelegateCASConfig;
import build.buildfarm.v1test.MemoryInstanceConfig;
import build.buildfarm.v1test.MemoryCASConfig;
import build.buildfarm.v1test.OperationQueueGrpc;
import build.buildfarm.v1test.TakeOperationRequest;
import build.buildfarm.v1test.PollOperationRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionCacheGrpc;
import build.bazel.remote.execution.v2.BatchUpdateBlobsRequest;
import build.bazel.remote.execution.v2.BatchUpdateBlobsRequest.Request;
import build.bazel.remote.execution.v2.BatchUpdateBlobsResponse;
import build.bazel.remote.execution.v2.BatchUpdateBlobsResponse.Response;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecuteRequest;
import build.bazel.remote.execution.v2.ExecutionGrpc;
import build.bazel.remote.execution.v2.FindMissingBlobsRequest;
import build.bazel.remote.execution.v2.FindMissingBlobsResponse;
import build.bazel.remote.execution.v2.GetActionResultRequest;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc;
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
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.util.Collections;
import java.util.function.Function;
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
        .setCasConfig(ContentAddressableStorageConfig.newBuilder()
            .setMemory(MemoryCASConfig.newBuilder()
                .setMaxSizeBytes(640 * 1024)))
        .setActionCacheConfig(ActionCacheConfig.newBuilder()
            .setDelegateCas(DelegateCASConfig.getDefaultInstance())
            .build())
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
        .setDigestFunction(DigestFunction.SHA256)
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
    DigestUtil digestUtil = new DigestUtil(HashFunction.SHA256);
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
    DigestUtil digestUtil = new DigestUtil(HashFunction.SHA256);
    ByteString content = ByteString.copyFromUtf8("Hello, World!");
    Digest digest = digestUtil.compute(content);
    BatchUpdateBlobsRequest request = BatchUpdateBlobsRequest.newBuilder()
        .setInstanceName("memory")
        .addRequests(Request.newBuilder()
            .setDigest(digest)
            .setData(content)
            .build())
        .build();
    ContentAddressableStorageGrpc.ContentAddressableStorageBlockingStub stub =
        ContentAddressableStorageGrpc.newBlockingStub(inProcessChannel);

    BatchUpdateBlobsResponse response = stub.batchUpdateBlobs(request);

    Response expected = Response.newBuilder()
        .setDigest(digest)
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
    GetOperationRequest getRequest = GetOperationRequest.newBuilder()
        .setName(operation.getName())
        .build();

    Operation preCancelOperation = operationsStub.getOperation(getRequest);

    assertThat(preCancelOperation.getDone()).isFalse();

    CancelOperationRequest cancelRequest = CancelOperationRequest.newBuilder()
        .setName(operation.getName())
        .build();

    operationsStub.cancelOperation(cancelRequest);

    Operation cancelledOperation = operationsStub.getOperation(getRequest);

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

  @Test(expected = StatusRuntimeException.class)
  public void actionWithExcessiveTimeoutFailsValidation()
      throws RetryException, InterruptedException, InvalidProtocolBufferException {
    Digest actionDigestWithExcessiveTimeout = createAction(Action.newBuilder()
        .setTimeout(Duration.newBuilder().setSeconds(9000)));

    executeAction(actionDigestWithExcessiveTimeout);
  }

  private Digest createSimpleAction() throws RetryException, InterruptedException {
    return createAction(Action.newBuilder());
  }

  private Digest createAction(Action.Builder actionBuilder) throws RetryException, InterruptedException {
    DigestUtil digestUtil = new DigestUtil(HashFunction.SHA256);
    Command command = Command.newBuilder()
        .addArguments("echo")
        .build();
    Digest commandBlobDigest = digestUtil.compute(command);
    Directory root = Directory.getDefaultInstance();
    Digest rootBlobDigest = digestUtil.compute(root);
    Action action = actionBuilder
        .setCommandDigest(commandBlobDigest)
        .setInputRootDigest(rootBlobDigest)
        .build();
    Digest actionDigest = digestUtil.compute(action);
    ByteStreamUploader uploader = new ByteStreamUploader("memory", inProcessChannel, null, 60, Retrier.NO_RETRIES, null);

    uploader.uploadBlobs(ImmutableList.of(
        new Chunker(action.toByteString(), actionDigest),
        new Chunker(command.toByteString(), commandBlobDigest)));
    return actionDigest;
  }

  private Operation executeAction(Digest actionDigest) {
    ExecuteRequest executeRequest = ExecuteRequest.newBuilder()
        .setInstanceName("memory")
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
