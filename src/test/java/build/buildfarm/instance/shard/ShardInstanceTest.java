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

package build.buildfarm.instance.shard;

import static build.bazel.remote.execution.v2.ExecuteOperationMetadata.Stage.UNKNOWN;
import static build.bazel.remote.execution.v2.ExecuteOperationMetadata.Stage.CACHE_CHECK;
import static build.bazel.remote.execution.v2.ExecuteOperationMetadata.Stage.QUEUED;
import static build.bazel.remote.execution.v2.ExecuteOperationMetadata.Stage.COMPLETED;
import static build.buildfarm.instance.AbstractServerInstance.MISSING_INPUT;
import static build.buildfarm.instance.AbstractServerInstance.VIOLATION_TYPE_MISSING;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.grpc.Status.Code.CANCELLED;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.bazel.remote.execution.v2.OutputFile;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.ShardBackplane;
import build.buildfarm.instance.Instance.CommittingOutputStream;
import build.buildfarm.instance.Instance;
import build.buildfarm.v1test.QueuedOperationMetadata;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.rpc.PreconditionFailure;
import com.google.rpc.PreconditionFailure.Violation;
import io.grpc.Status.Code;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

@RunWith(JUnit4.class)
public class ShardInstanceTest {
  private static final DigestUtil DIGEST_UTIL = new DigestUtil(HashFunction.SHA256);
  private static final long QUEUE_TEST_TIMEOUT_SECONDS = 3;

  private ShardInstance instance;

  @Mock
  private ShardBackplane mockBackplane;

  @Mock
  private Runnable mockOnStop;

  @Mock
  private CacheLoader<String, Instance> mockInstanceLoader;

  @Mock
  Instance mockWorkerInstance;

  @Before
  public void setUp() throws InterruptedException {
    MockitoAnnotations.initMocks(this);
    instance = new ShardInstance(
        "shard",
        DIGEST_UTIL,
        mockBackplane,
        /* runDispatchedMonitor=*/ false,
        /* runOperationQueuer=*/ false,
        mockOnStop,
        mockInstanceLoader);
    instance.start();
  }

  @After
  public void tearDown() throws InterruptedException {
    instance.stop();
  }

  private Action createAction(boolean provideAction) throws Exception {
    String workerName = "worker";
    when(mockInstanceLoader.load(eq(workerName))).thenReturn(mockWorkerInstance);

    Directory inputRoot = Directory.getDefaultInstance();
    Digest inputRootDigest = DIGEST_UTIL.compute(inputRoot);
    when(mockBackplane.getTree(eq(inputRootDigest))).thenReturn(ImmutableList.of(inputRoot));

    ImmutableSet<String> workers = ImmutableSet.of(workerName);
    when(mockBackplane.getWorkers()).thenReturn(workers);

    Command command = Command.newBuilder()
        .addAllArguments(ImmutableList.of("true"))
        .build();
    ByteString commandBlob = command.toByteString();
    Digest commandDigest = DIGEST_UTIL.compute(commandBlob);

    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) {
        StreamObserver<ByteString> blobObserver = (StreamObserver) invocation.getArguments()[3];
        blobObserver.onNext(commandBlob);
        blobObserver.onCompleted();
        return null;
      }
    }).when(mockWorkerInstance).getBlob(eq(commandDigest), eq(0l), eq(0l), any(StreamObserver.class));
    when(mockBackplane.getBlobLocationSet(eq(commandDigest))).thenReturn(workers);

    // janky - need a better supplier here
    // when(mockWorkerInstance.putBlob(eq(commandBlob))).thenReturn(commandDigest);
    when(mockWorkerInstance.findMissingBlobs(eq(ImmutableList.of(commandDigest)), any(ExecutorService.class))).thenReturn(immediateFuture(ImmutableList.of()));

    Action action = Action.newBuilder()
        .setCommandDigest(commandDigest)
        .setInputRootDigest(DIGEST_UTIL.compute(inputRoot))
        .build();

    Digest actionDigest = DIGEST_UTIL.compute(action);
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) {
        StreamObserver<ByteString> blobObserver = (StreamObserver) invocation.getArguments()[3];
        if (provideAction) {
          blobObserver.onNext(action.toByteString());
          blobObserver.onCompleted();
        } else {
          blobObserver.onError(Status.NOT_FOUND.asException());
        }
        return null;
      }
    }).when(mockWorkerInstance).getBlob(eq(actionDigest), eq(0l), eq(0l), any(StreamObserver.class));
    when(mockBackplane.getBlobLocationSet(eq(actionDigest))).thenReturn(provideAction ? workers : ImmutableSet.of());
    when(mockWorkerInstance.findMissingBlobs(eq(ImmutableList.of(actionDigest)), any(ExecutorService.class))).thenReturn(immediateFuture(ImmutableList.of()));

    return action;
  }

  @Test
  public void queueActionMissingErrorsOperation() throws Exception {
    Action action = createAction(false);
    Digest actionDigest = DIGEST_UTIL.compute(action);

    Operation operation = Operation.newBuilder()
        .setName("operation")
        .setMetadata(Any.pack(QueuedOperationMetadata.newBuilder()
            .setExecuteOperationMetadata(ExecuteOperationMetadata.newBuilder()
                .setActionDigest(actionDigest))
            .setSkipCacheLookup(true)
            .build()))
        .build();

    when(mockBackplane.getOperation(eq(operation.getName()))).thenReturn(operation);
    when(mockBackplane.canQueue()).thenReturn(true);

    boolean unknownExceptionCaught = false;
    try {
      instance.queue(operation).get(QUEUE_TEST_TIMEOUT_SECONDS, SECONDS);
    } catch (ExecutionException e) {
      Status status = Status.fromThrowable(e);
      if (status.getCode() == Code.FAILED_PRECONDITION) {
        unknownExceptionCaught = true;
      } else {
        e.getCause().printStackTrace();
      }
    }
    assertThat(unknownExceptionCaught).isTrue();

    ExecuteResponse executeResponse = ExecuteResponse.newBuilder()
        .setStatus(com.google.rpc.Status.newBuilder()
            .setCode(Code.FAILED_PRECONDITION.value())
            .setMessage("FAILED_PRECONDITION: NOT_FOUND")
            .addDetails(Any.pack(PreconditionFailure.newBuilder()
                .addViolations(Violation.newBuilder()
                    .setType(VIOLATION_TYPE_MISSING)
                    .setSubject(MISSING_INPUT)
                    .setDescription("Action " + DigestUtil.toString(actionDigest)))
                .build())))
        .build();
    Operation erroredOperation = operation.toBuilder()
        .setDone(true)
        .setMetadata(Any.pack(ExecuteOperationMetadata.newBuilder()
            .setActionDigest(actionDigest)
            .setStage(COMPLETED)
            .build()))
        .setResponse(Any.pack(executeResponse))
        .build();
    verify(mockBackplane, times(1)).putOperation(eq(erroredOperation), eq(COMPLETED));
  }

  @Test
  public void queueOperationPutFailureCancelsOperation() throws Exception {
    Action action = createAction(true);
    Digest actionDigest = DIGEST_UTIL.compute(action);
    CommittingOutputStream mockCommittedOutputStream = mock(CommittingOutputStream.class);
    when(mockCommittedOutputStream.getCommittedFuture()).thenReturn(immediateFuture(actionDigest.getSizeBytes()));

    when(mockWorkerInstance.findMissingBlobs(any(Iterable.class), any(ExecutorService.class))).thenReturn(immediateFuture(ImmutableList.of()));

    when(mockWorkerInstance.getStreamOutput(
        matches("^uploads/.* /blobs/" + DigestUtil.toString(actionDigest) + "$"),
        eq(actionDigest.getSizeBytes()))).thenReturn(mockCommittedOutputStream);

    StatusRuntimeException queueException = Status.UNAVAILABLE.asRuntimeException();
    when(mockBackplane.putOperation(any(Operation.class), eq(QUEUED)))
        .thenThrow(new IOException(queueException));

    Operation operation = Operation.newBuilder()
        .setName("queueOperationPutFailureCancelsOperation")
        .setMetadata(Any.pack(QueuedOperationMetadata.newBuilder()
            .setExecuteOperationMetadata(ExecuteOperationMetadata.newBuilder()
                .setActionDigest(actionDigest))
            .setSkipCacheLookup(true)
            .build()))
        .build();

    when(mockBackplane.getOperation(eq(operation.getName()))).thenReturn(operation);
    when(mockBackplane.canQueue()).thenReturn(true);

    boolean unavailableExceptionCaught = false;
    try {
      // anything more would be unreasonable
      instance.queue(operation).get(QUEUE_TEST_TIMEOUT_SECONDS, SECONDS);
    } catch (ExecutionException e) {
      Status status = Status.fromThrowable(e);
      if (status.getCode() == Code.UNAVAILABLE) {
        unavailableExceptionCaught = true;
      } else {
        throw e;
      }
    }
    assertThat(unavailableExceptionCaught).isTrue();

    verify(mockBackplane, times(1)).putOperation(any(Operation.class), eq(QUEUED));
    ExecuteResponse executeResponse = ExecuteResponse.newBuilder()
        .setStatus(com.google.rpc.Status.newBuilder()
            .setCode(queueException.getStatus().getCode().value())
            .setMessage(queueException.getMessage()))
        .build();
    Operation erroredOperation = operation.toBuilder()
        .setDone(true)
        .setMetadata(Any.pack(ExecuteOperationMetadata.newBuilder()
            .setActionDigest(actionDigest)
            .setStage(COMPLETED)
            .build()))
        .setResponse(Any.pack(executeResponse))
        .build();
    verify(mockBackplane, times(1)).putOperation(eq(erroredOperation), eq(COMPLETED));
  }

  @Test
  public void queueOperationCompletesOperationWithCachedActionResult() throws Exception {
    ActionKey actionKey = DigestUtil.asActionKey(Digest.newBuilder()
        .setHash("test")
        .build());
    Operation operation = Operation.newBuilder()
        .setName("operationWithCachedActionResult")
        .setMetadata(Any.pack(QueuedOperationMetadata.newBuilder()
            .setExecuteOperationMetadata(ExecuteOperationMetadata.newBuilder()
                .setActionDigest(actionKey.getDigest()))
            .build()))
        .build();

    ActionResult actionResult = ActionResult.getDefaultInstance();

    when(mockBackplane.getOperation(eq(operation.getName()))).thenReturn(operation);
    when(mockBackplane.getActionResult(eq(actionKey))).thenReturn(actionResult);

    instance.queue(operation).get();

    verify(mockBackplane, times(1)).putOperation(any(Operation.class), eq(CACHE_CHECK));
    verify(mockBackplane, never()).putOperation(any(Operation.class), eq(QUEUED));
    verify(mockBackplane, times(1)).putOperation(any(Operation.class), eq(COMPLETED));
  }

  @Test
  public void actionResultsWithMissingOutputsAreInvalidated() throws IOException {
    ActionKey actionKey = DigestUtil.asActionKey(Digest.newBuilder()
        .setHash("test")
        .build());
    ActionResult actionResult = ActionResult.newBuilder()
        .addOutputFiles(
            OutputFile.newBuilder()
                .setPath("does-not-exist")
                .setDigest(
                    Digest.newBuilder()
                        .setHash("dne")
                        .setSizeBytes(1)))
        .build();

    when(mockBackplane.getActionResult(eq(actionKey))).thenReturn(actionResult);

    assertThat(instance.getActionResult(actionKey)).isNull();
    verify(mockBackplane, times(1)).removeActionResult(eq(actionKey));
  }
}
