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
import static build.buildfarm.instance.AbstractServerInstance.MISSING_ACTION;
import static build.buildfarm.instance.AbstractServerInstance.MISSING_COMMAND;
import static build.buildfarm.instance.AbstractServerInstance.MISSING_INPUT;
import static build.buildfarm.instance.AbstractServerInstance.VIOLATION_TYPE_MISSING;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
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
import build.buildfarm.common.Poller;
import build.buildfarm.common.ShardBackplane;
import build.buildfarm.instance.Instance.CommittingOutputStream;
import build.buildfarm.instance.Instance;
import build.buildfarm.v1test.ExecuteEntry;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.QueuedOperation;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.rpc.Code;
import com.google.rpc.PreconditionFailure;
import com.google.rpc.PreconditionFailure.Violation;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

@RunWith(JUnit4.class)
public class ShardInstanceTest {
  private static final DigestUtil DIGEST_UTIL = new DigestUtil(HashFunction.SHA256);
  private static final long QUEUE_TEST_TIMEOUT_SECONDS = 3;
  private static final Command SIMPLE_COMMAND = Command.newBuilder()
      .addAllArguments(ImmutableList.of("true"))
      .build();

  private ShardInstance instance;
  private Set<Digest> blobDigests;

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
    blobDigests = Sets.newHashSet();
    instance = new ShardInstance(
        "shard",
        DIGEST_UTIL,
        mockBackplane,
        /* runDispatchedMonitor=*/ false,
        /* runOperationQueuer=*/ false,
        mockOnStop,
        CacheBuilder.newBuilder()
            .build(mockInstanceLoader));
    instance.start();
  }

  @After
  public void tearDown() throws InterruptedException {
    instance.stop();
  }

  private Action createAction() throws Exception {
    return createAction(/* provideAction=*/ true, /* provideCommand=*/ true, SIMPLE_COMMAND);
  }

  private Action createAction(boolean provideAction) throws Exception {
    return createAction(provideAction, /* provideCommand=*/ true, SIMPLE_COMMAND);
  }

  private Action createAction(boolean provideAction, boolean provideCommand) throws Exception {
    return createAction(provideAction, provideCommand, SIMPLE_COMMAND);
  }

  private Action createAction(boolean provideAction, boolean provideCommand, Command command) throws Exception {
    Directory inputRoot = Directory.getDefaultInstance();
    ByteString inputRootBlob = inputRoot.toByteString();
    Digest inputRootDigest = DIGEST_UTIL.compute(inputRootBlob);
    provideBlob(inputRootDigest, inputRootBlob);

    return createAction(provideAction, provideCommand, inputRootDigest, command);
  }

  private Action createAction(boolean provideAction, boolean provideCommand, Digest inputRootDigest, Command command) throws Exception {
    String workerName = "worker";
    when(mockInstanceLoader.load(eq(workerName))).thenReturn(mockWorkerInstance);

    ImmutableSet<String> workers = ImmutableSet.of(workerName);
    when(mockBackplane.getWorkers()).thenReturn(workers);

    ByteString commandBlob = command.toByteString();
    Digest commandDigest = DIGEST_UTIL.compute(commandBlob);
    if (provideCommand) {
      provideBlob(commandDigest, commandBlob);
      when(mockBackplane.getBlobLocationSet(eq(commandDigest))).thenReturn(workers);
    }

    doAnswer(new Answer<ListenableFuture<Iterable<Digest>>>() {
      @Override
      public ListenableFuture<Iterable<Digest>> answer(InvocationOnMock invocation) {
        Iterable<Digest> digests = (Iterable<Digest>) invocation.getArguments()[0];
        return immediateFuture(Iterables.filter(digests, (digest) -> !blobDigests.contains(digest)));
      }
    }).when(mockWorkerInstance).findMissingBlobs(any(Iterable.class), any(ExecutorService.class));

    Action action = Action.newBuilder()
        .setCommandDigest(commandDigest)
        .setInputRootDigest(inputRootDigest)
        .build();

    ByteString actionBlob = action.toByteString();
    Digest actionDigest = DIGEST_UTIL.compute(actionBlob);
    if (provideAction) {
      provideBlob(actionDigest, actionBlob);
    }

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

    ExecuteEntry executeEntry = ExecuteEntry.newBuilder()
        .setOperationName("missing-action-operation")
        .setActionDigest(actionDigest)
        .setSkipCacheLookup(true)
        .build();

    when(mockBackplane.canQueue()).thenReturn(true);

    Poller poller = mock(Poller.class);

    boolean failedPreconditionExceptionCaught = false;
    try {
      instance.queue(executeEntry, poller)
          .get(QUEUE_TEST_TIMEOUT_SECONDS, SECONDS);
    } catch (ExecutionException e) {
      com.google.rpc.Status status = StatusProto.fromThrowable(e);
      if (status.getCode() == Code.FAILED_PRECONDITION.getNumber()) {
        failedPreconditionExceptionCaught = true;
      } else {
        e.getCause().printStackTrace();
      }
    }
    assertThat(failedPreconditionExceptionCaught).isTrue();

    ExecuteResponse executeResponse = ExecuteResponse.newBuilder()
        .setStatus(com.google.rpc.Status.newBuilder()
            .setCode(Code.FAILED_PRECONDITION.getNumber())
            .setMessage(ShardInstance.invalidActionMessage(actionDigest))
            .addDetails(Any.pack(PreconditionFailure.newBuilder()
                .addViolations(Violation.newBuilder()
                    .setType(VIOLATION_TYPE_MISSING)
                    .setSubject("blobs/" + DigestUtil.toString(actionDigest))
                    .setDescription(MISSING_ACTION))
                .build())))
        .build();
    Operation erroredOperation = Operation.newBuilder()
        .setName(executeEntry.getOperationName())
        .setDone(true)
        .setMetadata(Any.pack(ExecuteOperationMetadata.newBuilder()
            .setActionDigest(actionDigest)
            .setStage(COMPLETED)
            .build()))
        .setResponse(Any.pack(executeResponse))
        .build();
    verify(mockBackplane, times(1)).putOperation(eq(erroredOperation), eq(COMPLETED));
    verify(poller, atLeastOnce()).pause();
  }

  @Test
  public void queueCommandMissingErrorsOperation() throws Exception {
    Action action = createAction(true, false);
    Digest actionDigest = DIGEST_UTIL.compute(action);

    ExecuteEntry executeEntry = ExecuteEntry.newBuilder()
        .setOperationName("missing-command-operation")
        .setActionDigest(actionDigest)
        .setSkipCacheLookup(true)
        .build();

    when(mockBackplane.canQueue()).thenReturn(true);

    Poller poller = mock(Poller.class);

    boolean failedPreconditionExceptionCaught = false;
    try {
      instance.queue(executeEntry, poller)
          .get(QUEUE_TEST_TIMEOUT_SECONDS, SECONDS);
    } catch (ExecutionException e) {
      com.google.rpc.Status status = StatusProto.fromThrowable(e);
      if (status.getCode() == Code.FAILED_PRECONDITION.getNumber()) {
        failedPreconditionExceptionCaught = true;
      } else {
        e.getCause().printStackTrace();
      }
    }
    assertThat(failedPreconditionExceptionCaught).isTrue();

    ExecuteResponse executeResponse = ExecuteResponse.newBuilder()
        .setStatus(com.google.rpc.Status.newBuilder()
            .setCode(Code.FAILED_PRECONDITION.getNumber())
            .setMessage(ShardInstance.invalidActionMessage(actionDigest))
            .addDetails(Any.pack(PreconditionFailure.newBuilder()
                .addViolations(Violation.newBuilder()
                    .setType(VIOLATION_TYPE_MISSING)
                    .setSubject("blobs/" + DigestUtil.toString(action.getCommandDigest()))
                    .setDescription(MISSING_COMMAND))
                .build())))
        .build();
    Operation erroredOperation = Operation.newBuilder()
        .setName(executeEntry.getOperationName())
        .setDone(true)
        .setMetadata(Any.pack(ExecuteOperationMetadata.newBuilder()
            .setActionDigest(actionDigest)
            .setStage(COMPLETED)
            .build()))
        .setResponse(Any.pack(executeResponse))
        .build();
    verify(mockBackplane, times(1)).putOperation(eq(erroredOperation), eq(COMPLETED));
    verify(poller, atLeastOnce()).pause();
  }

  @Test
  public void queueOperationPutFailureCancelsOperation() throws Exception {
    Action action = createAction();
    Digest actionDigest = DIGEST_UTIL.compute(action);
    CommittingOutputStream mockCommittedOutputStream = mock(CommittingOutputStream.class);
    when(mockCommittedOutputStream.getCommittedFuture()).thenReturn(immediateFuture(actionDigest.getSizeBytes()));

    when(mockWorkerInstance.findMissingBlobs(any(Iterable.class), any(ExecutorService.class))).thenReturn(immediateFuture(ImmutableList.of()));

    when(mockWorkerInstance.getStreamOutput(
        matches("^uploads/[^/]+/blobs/.*$"),
        any(Long.class))).thenReturn(mockCommittedOutputStream);

    StatusRuntimeException queueException = Status.UNAVAILABLE.asRuntimeException();
    doAnswer((invocation) -> {
      throw new IOException(queueException);
    }).when(mockBackplane).queue(any(QueueEntry.class), any(Operation.class));

    ExecuteEntry executeEntry = ExecuteEntry.newBuilder()
        .setOperationName("queueOperationPutFailureCancelsOperation")
        .setActionDigest(actionDigest)
        .setSkipCacheLookup(true)
        .build();

    when(mockBackplane.canQueue()).thenReturn(true);

    Poller poller = mock(Poller.class);

    boolean unavailableExceptionCaught = false;
    try {
      // anything more would be unreasonable
      instance.queue(executeEntry, poller)
          .get(QUEUE_TEST_TIMEOUT_SECONDS, SECONDS);
    } catch (ExecutionException e) {
      com.google.rpc.Status status = StatusProto.fromThrowable(e);
      if (status.getCode() == Code.UNAVAILABLE.getNumber()) {
        unavailableExceptionCaught = true;
      } else {
        throw e;
      }
    }
    assertThat(unavailableExceptionCaught).isTrue();

    verify(mockBackplane, times(1)).queue(any(QueueEntry.class), any(Operation.class));
    ExecuteResponse executeResponse = ExecuteResponse.newBuilder()
        .setStatus(com.google.rpc.Status.newBuilder()
            .setCode(queueException.getStatus().getCode().value())
            .setMessage(queueException.getMessage()))
        .build();
    Operation erroredOperation = Operation.newBuilder()
        .setName(executeEntry.getOperationName())
        .setDone(true)
        .setMetadata(Any.pack(ExecuteOperationMetadata.newBuilder()
            .setActionDigest(actionDigest)
            .setStage(COMPLETED)
            .build()))
        .setResponse(Any.pack(executeResponse))
        .build();
    verify(mockBackplane, times(1)).putOperation(eq(erroredOperation), eq(COMPLETED));
    verify(poller, atLeastOnce()).pause();
  }

  @Test
  public void queueOperationCompletesOperationWithCachedActionResult() throws Exception {
    ActionKey actionKey = DigestUtil.asActionKey(Digest.newBuilder()
        .setHash("test")
        .build());
    ExecuteEntry executeEntry = ExecuteEntry.newBuilder()
        .setOperationName("operationWithCachedActionResult")
        .setActionDigest(actionKey.getDigest())
        .build();

    ActionResult actionResult = ActionResult.getDefaultInstance();

    when(mockBackplane.getActionResult(eq(actionKey))).thenReturn(actionResult);

    Poller poller = mock(Poller.class);

    instance.queue(executeEntry, poller).get();

    verify(mockBackplane, times(1)).putOperation(any(Operation.class), eq(CACHE_CHECK));
    verify(mockBackplane, never()).putOperation(any(Operation.class), eq(QUEUED));
    verify(mockBackplane, times(1)).putOperation(any(Operation.class), eq(COMPLETED));
    verify(poller, atLeastOnce()).pause();
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

  @Test
  public void requeueFailsOnMissingDirectory() throws Exception {
    String operationName = "missing-directory-operation";

    Digest missingDirectoryDigest = Digest.newBuilder()
        .setHash("missing-directory")
        .setSizeBytes(1)
        .build();

    when(mockBackplane.getOperation(eq(operationName))).thenReturn(
        Operation.newBuilder()
            .setName(operationName)
            .setMetadata(Any.pack(ExecuteOperationMetadata.newBuilder()
                .setStage(QUEUED)
                .build()))
            .build());

    Action action = createAction(true, true, missingDirectoryDigest, SIMPLE_COMMAND);
    Digest actionDigest = DIGEST_UTIL.compute(action);
    QueueEntry queueEntry = QueueEntry.newBuilder()
        .setExecuteEntry(ExecuteEntry.newBuilder()
            .setOperationName(operationName)
            .setSkipCacheLookup(true)
            .setActionDigest(actionDigest))
        .build();
    instance.requeueOperation(queueEntry).get();
    ArgumentCaptor<Operation> operationCaptor = ArgumentCaptor.forClass(Operation.class);
    verify(mockBackplane, times(1)).putOperation(operationCaptor.capture(), eq(COMPLETED));
    Operation operation = operationCaptor.getValue();
    assertThat(operation.getResponse().is(ExecuteResponse.class)).isTrue();
    ExecuteResponse executeResponse = operation.getResponse().unpack(ExecuteResponse.class);
    com.google.rpc.Status status = executeResponse.getStatus();
    com.google.rpc.Status expectedStatus = com.google.rpc.Status.newBuilder()
        .setCode(Code.FAILED_PRECONDITION.getNumber())
        .setMessage(ShardInstance.invalidActionMessage(actionDigest))
        .addDetails(Any.pack(PreconditionFailure.newBuilder()
            .addViolations(Violation.newBuilder()
                .setType(VIOLATION_TYPE_MISSING)
                .setSubject("blobs/" + DigestUtil.toString(missingDirectoryDigest))
                .setDescription("The directory `/` was not found in the CAS."))
            .build()))
        .build();
    assertThat(status).isEqualTo(expectedStatus);
  }

  private void provideBlob(Digest digest, ByteString content) {
    blobDigests.add(digest);
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) {
        StreamObserver<ByteString> blobObserver = (StreamObserver) invocation.getArguments()[3];
        blobObserver.onNext(content);
        blobObserver.onCompleted();
        return null;
      }
    }).when(mockWorkerInstance).getBlob(eq(digest), eq(0l), eq(0l), any(StreamObserver.class));
  }

  @Test
  public void requeueSucceedsForValidOperation() throws Exception {
    String operationName = "valid-operation";

    when(mockBackplane.getOperation(eq(operationName))).thenReturn(
        Operation.newBuilder()
            .setName(operationName)
            .build());

    Action action = createAction();
    QueuedOperation queuedOperation = QueuedOperation.newBuilder()
        .setAction(action)
        .setCommand(SIMPLE_COMMAND)
        .build();
    ByteString queuedOperationBlob = queuedOperation.toByteString();
    Digest queuedOperationDigest = DIGEST_UTIL.compute(queuedOperationBlob);
    provideBlob(queuedOperationDigest, queuedOperationBlob);

    Digest actionDigest = DIGEST_UTIL.compute(action);
    QueueEntry queueEntry = QueueEntry.newBuilder()
        .setExecuteEntry(ExecuteEntry.newBuilder()
            .setOperationName(operationName)
            .setSkipCacheLookup(true)
            .setActionDigest(actionDigest))
        .setQueuedOperationDigest(queuedOperationDigest)
        .build();
    instance.requeueOperation(queueEntry).get();
  }

  @Test
  public void blobsAreMissingWhenWorkersIsEmpty() throws Exception {
    when(mockBackplane.getWorkers()).thenReturn(ImmutableSet.of());
    Digest digest = Digest.newBuilder()
        .setHash("hash")
        .setSizeBytes(1)
        .build();
    Iterable<Digest> missingDigests = instance.findMissingBlobs(
        ImmutableList.of(digest),
        newDirectExecutorService()).get();
    assertThat(missingDigests).containsExactly(digest);
  }
}
