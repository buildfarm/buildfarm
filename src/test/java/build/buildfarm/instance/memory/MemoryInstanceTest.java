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

package build.buildfarm.instance.memory;

import static build.bazel.remote.execution.v2.ExecutionStage.Value.CACHE_CHECK;
import static build.bazel.remote.execution.v2.ExecutionStage.Value.COMPLETED;
import static build.bazel.remote.execution.v2.ExecutionStage.Value.EXECUTING;
import static build.bazel.remote.execution.v2.ExecutionStage.Value.QUEUED;
import static build.bazel.remote.execution.v2.ExecutionStage.Value.UNKNOWN;
import static build.buildfarm.cas.ContentAddressableStorages.casMapDecorator;
import static build.buildfarm.common.Actions.invalidActionVerboseMessage;
import static build.buildfarm.common.Errors.VIOLATION_TYPE_INVALID;
import static build.buildfarm.common.Errors.VIOLATION_TYPE_MISSING;
import static build.buildfarm.instance.memory.MemoryInstance.TIMEOUT_OUT_OF_BOUNDS;
import static build.buildfarm.instance.server.AbstractServerInstance.MISSING_ACTION;
import static com.google.common.collect.Multimaps.synchronizedSetMultimap;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.bazel.remote.execution.v2.ExecutionPolicy;
import build.bazel.remote.execution.v2.ExecutionStage;
import build.bazel.remote.execution.v2.Platform;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.bazel.remote.execution.v2.ResultsCachePolicy;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.Watchdog;
import build.buildfarm.common.Watcher;
import build.buildfarm.instance.MatchListener;
import build.buildfarm.instance.queues.Worker;
import build.buildfarm.instance.server.OperationsMap;
import build.buildfarm.instance.server.WatchFuture;
import build.buildfarm.v1test.ActionCacheConfig;
import build.buildfarm.v1test.CompletedOperationMetadata;
import build.buildfarm.v1test.DelegateCASConfig;
import build.buildfarm.v1test.MemoryInstanceConfig;
import build.buildfarm.v1test.QueueEntry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Durations;
import com.google.rpc.Code;
import com.google.rpc.PreconditionFailure;
import com.google.rpc.PreconditionFailure.Violation;
import com.google.rpc.Status;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

@RunWith(JUnit4.class)
public class MemoryInstanceTest {
  private final DigestUtil DIGEST_UTIL = new DigestUtil(DigestUtil.HashFunction.SHA256);
  private final Command simpleCommand = Command.newBuilder().addArguments("true").build();
  private final Digest simpleCommandDigest = DIGEST_UTIL.compute(simpleCommand);
  private final Action simpleAction =
      Action.newBuilder().setCommandDigest(simpleCommandDigest).build();
  private final Digest simpleActionDigest = DIGEST_UTIL.compute(simpleAction);
  private final Duration MAXIMUM_ACTION_TIMEOUT = Durations.fromSeconds(3600);

  private MemoryInstance instance;

  private OperationsMap outstandingOperations;
  private SetMultimap<String, WatchFuture> watchers;
  private ExecutorService watcherService;

  private Map<String, ByteString> storage;
  private List<Worker> workers;
  private Map<String, Watchdog> requeuers;
  private Map<String, Watchdog> operationTimeoutDelays;

  @Before
  public void setUp() throws Exception {
    outstandingOperations = new MemoryInstance.OutstandingOperations();
    watchers =
        synchronizedSetMultimap(
            MultimapBuilder.hashKeys().hashSetValues(/* expectedValuesPerKey=*/ 1).build());
    watcherService = newDirectExecutorService();
    MemoryInstanceConfig memoryInstanceConfig =
        MemoryInstanceConfig.newBuilder()
            .setListOperationsDefaultPageSize(1024)
            .setListOperationsMaxPageSize(16384)
            .setTreeDefaultPageSize(1024)
            .setTreeMaxPageSize(16384)
            .setOperationPollTimeout(Durations.fromSeconds(10))
            .setOperationCompletedDelay(Durations.fromSeconds(10))
            .setDefaultActionTimeout(Durations.fromSeconds(600))
            .setMaximumActionTimeout(MAXIMUM_ACTION_TIMEOUT)
            .setActionCacheConfig(
                ActionCacheConfig.newBuilder()
                    .setDelegateCas(DelegateCASConfig.getDefaultInstance())
                    .build())
            .build();

    storage = Maps.newHashMap();
    workers = Lists.newArrayList();
    requeuers = Maps.newHashMap();
    operationTimeoutDelays = Maps.newHashMap();
    instance =
        new MemoryInstance(
            "memory",
            DIGEST_UTIL,
            memoryInstanceConfig,
            casMapDecorator(storage),
            watchers,
            watcherService,
            outstandingOperations,
            workers,
            requeuers,
            operationTimeoutDelays);
  }

  @After
  public void tearDown() throws Exception {
    watcherService.shutdown();
    watcherService.awaitTermination(Long.MAX_VALUE, NANOSECONDS);
  }

  @Test
  public void listOperationsForEmptyOutstanding() {
    ImmutableList.Builder<Operation> operations = new ImmutableList.Builder<>();

    String nextToken =
        instance.listOperations(/* pageSize=*/ 1, /* pageToken=*/ "", /* filter=*/ "", operations);
    assertThat(nextToken).isEqualTo("");
    assertThat(operations.build()).isEmpty();
  }

  @Test
  public void listOperationsForOutstandingOperations() throws InterruptedException {
    Operation operation = Operation.newBuilder().setName("test-operation").build();

    outstandingOperations.put(operation.getName(), operation);

    ImmutableList.Builder<Operation> operations = new ImmutableList.Builder<>();

    String nextToken =
        instance.listOperations(/* pageSize=*/ 1, /* pageToken=*/ "", /* filter=*/ "", operations);
    // we should have reached the end of list
    assertThat(nextToken).isEqualTo("");
    assertThat(operations.build()).containsExactly(operation);
  }

  @Test
  public void listOperationsLimitsPages() throws InterruptedException {
    Operation testOperation1 = Operation.newBuilder().setName("test-operation1").build();

    Operation testOperation2 = Operation.newBuilder().setName("test-operation2").build();

    outstandingOperations.put(testOperation1.getName(), testOperation1);
    outstandingOperations.put(testOperation2.getName(), testOperation2);

    ImmutableList.Builder<Operation> operations = new ImmutableList.Builder<>();

    String nextToken =
        instance.listOperations(/* pageSize=*/ 1, /* pageToken=*/ "", /* filter=*/ "", operations);
    // we should not be at the end
    assertThat(nextToken).isNotEqualTo("");
    assertThat(operations.build().size()).isEqualTo(1);

    nextToken =
        instance.listOperations(
            /* pageSize=*/ 1, /* pageToken=*/ nextToken, /* filter=*/ "", operations);
    // we should have reached the end
    assertThat(nextToken).isEqualTo("");
    assertThat(operations.build()).containsExactly(testOperation1, testOperation2);
  }

  @Test
  public void actionCacheMissResult() throws Exception {
    Action action = Action.getDefaultInstance();

    assertThat(
            instance
                .getActionResult(
                    instance.getDigestUtil().computeActionKey(action),
                    RequestMetadata.getDefaultInstance())
                .get())
        .isNull();
  }

  @Test
  public void actionCacheRetrievableByActionKey() throws Exception {
    ActionResult result = ActionResult.getDefaultInstance();
    storage.put(DIGEST_UTIL.compute(result).getHash(), result.toByteString());

    Action action = Action.getDefaultInstance();
    instance.putActionResult(instance.getDigestUtil().computeActionKey(action), result);
    assertThat(
            instance
                .getActionResult(
                    instance.getDigestUtil().computeActionKey(action),
                    RequestMetadata.getDefaultInstance())
                .get())
        .isEqualTo(result);
  }

  @Test
  public void missingOperationIsNotFound() throws InterruptedException {
    Watcher watcher = mock(Watcher.class);
    doThrow(io.grpc.Status.NOT_FOUND.asRuntimeException()).when(watcher).observe(eq(null));
    boolean caughtNotFoundException = false;
    try {
      instance.watchOperation("does-not-exist", watcher).get();
    } catch (ExecutionException e) {
      io.grpc.Status status = io.grpc.Status.fromThrowable(e);
      if (status.getCode() == io.grpc.Status.Code.NOT_FOUND) {
        caughtNotFoundException = true;
      }
    }
    assertThat(caughtNotFoundException).isTrue();
    verify(watcher, times(1)).observe(eq(null));
  }

  @Test
  public void watchOperationAddsWatcher() throws InterruptedException {
    Operation operation = Operation.newBuilder().setName("my-watched-operation").build();
    outstandingOperations.put(operation.getName(), operation);

    Watcher watcher = mock(Watcher.class);
    instance.watchOperation(operation.getName(), watcher);
    verify(watcher, times(1)).observe(eq(operation));
    WatchFuture watchFuture = Iterables.getOnlyElement(watchers.get(operation.getName()));
    watchFuture.observe(operation);
    verify(watcher, times(2)).observe(eq(operation));
  }

  @Test
  public void watchOpRaceLossInvertsTestOnInitial()
      throws ExecutionException, InterruptedException {
    Operation operation = Operation.newBuilder().setName("my-watched-operation").build();
    Operation doneOperation = operation.toBuilder().setDone(true).build();

    // as a result of the initial verified test, change the operation to done
    Answer<Void> initialAnswer =
        new Answer<Void>() {
          @Override
          public Void answer(InvocationOnMock invocation) throws Throwable {
            outstandingOperations.put(operation.getName(), doneOperation);
            return null;
          }
        };
    Watcher watcher = mock(Watcher.class);
    doAnswer(initialAnswer).when(watcher).observe(eq(operation));

    // set for each verification
    outstandingOperations.put(operation.getName(), operation);

    instance.watchOperation(operation.getName(), watcher).get();
    verify(watcher, times(1)).observe(eq(operation));
    verify(watcher, times(1)).observe(eq(doneOperation));
  }

  private class UnwatchFuture extends WatchFuture {
    private final String operationName;

    UnwatchFuture(String operationName, Watcher observer) {
      super(observer);
      this.operationName = operationName;
    }

    @Override
    public void unwatch() {
      watchers.remove(operationName, this);
    }
  }

  @Test
  public void requeueFailOnInvalid() throws InterruptedException, InvalidProtocolBufferException {
    // These new operations are invalid as they're missing actions.
    Operation queuedOperation = createOperation("missing-action-queued-operation", QUEUED);
    outstandingOperations.put(queuedOperation.getName(), queuedOperation);

    Watcher watcher = mock(Watcher.class);
    watchers.put(queuedOperation.getName(), new UnwatchFuture(queuedOperation.getName(), watcher));

    assertThat(instance.requeueOperation(queuedOperation)).isFalse();

    ArgumentCaptor<Operation> operationCaptor = ArgumentCaptor.forClass(Operation.class);
    verify(watcher, times(1)).observe(operationCaptor.capture());
    Operation operation = operationCaptor.getValue();
    assertThat(operation.getName()).isEqualTo(queuedOperation.getName());
  }

  @Test
  public void requeueSucceedsOnQueued()
      throws InterruptedException, InvalidProtocolBufferException {
    storage.put(simpleActionDigest.getHash(), simpleAction.toByteString());
    storage.put(simpleCommandDigest.getHash(), simpleCommand.toByteString());

    Operation queuedOperation = createOperation("my-queued-operation", QUEUED);
    outstandingOperations.put(queuedOperation.getName(), queuedOperation);
    Watcher watcher = mock(Watcher.class);
    watchers.put(queuedOperation.getName(), new UnwatchFuture(queuedOperation.getName(), watcher));

    assertThat(instance.requeueOperation(queuedOperation)).isTrue();
    verifyZeroInteractions(watcher);
  }

  @Test
  public void requeueFailureNotifiesWatchers() throws Exception {
    ExecuteOperationMetadata metadata =
        ExecuteOperationMetadata.newBuilder()
            .setActionDigest(simpleActionDigest)
            .setStage(QUEUED)
            .build();
    Operation queuedOperation = createOperation("missing-action-operation", metadata);
    outstandingOperations.put(queuedOperation.getName(), queuedOperation);
    Watcher watcher = mock(Watcher.class);
    ListenableFuture<Void> watchFuture =
        instance.watchOperation(queuedOperation.getName(), watcher);
    assertThat(instance.requeueOperation(queuedOperation)).isFalse();
    watchFuture.get();
    ArgumentCaptor<Operation> operationCaptor = ArgumentCaptor.forClass(Operation.class);
    verify(watcher, atLeastOnce()).observe(operationCaptor.capture());
    List<Operation> operations = operationCaptor.getAllValues();
    Operation erroredOperation = operations.get(operations.size() - 1);
    assertThat(erroredOperation.getDone()).isTrue();
    CompletedOperationMetadata completedMetadata =
        erroredOperation.getMetadata().unpack(CompletedOperationMetadata.class);
    assertThat(completedMetadata.getExecuteOperationMetadata().getStage()).isEqualTo(COMPLETED);
    PreconditionFailure preconditionFailure =
        PreconditionFailure.newBuilder()
            .addViolations(
                Violation.newBuilder()
                    .setType(VIOLATION_TYPE_MISSING)
                    .setSubject("blobs/" + DigestUtil.toString(simpleActionDigest))
                    .setDescription(MISSING_ACTION))
            .build();
    ExecuteResponse executeResponse =
        ExecuteResponse.newBuilder()
            .setStatus(
                Status.newBuilder()
                    .setCode(Code.FAILED_PRECONDITION.getNumber())
                    .setMessage(
                        invalidActionVerboseMessage(simpleActionDigest, preconditionFailure))
                    .addDetails(Any.pack(preconditionFailure)))
            .build();
    assertThat(erroredOperation.getResponse().unpack(ExecuteResponse.class))
        .isEqualTo(executeResponse);
  }

  private Operation createOperation(String name, ExecuteOperationMetadata metadata) {
    return Operation.newBuilder().setName(name).setMetadata(Any.pack(metadata)).build();
  }

  private Operation createOperation(String name, ExecutionStage.Value stage) {
    return createOperation(
        name,
        ExecuteOperationMetadata.newBuilder()
            .setActionDigest(simpleActionDigest)
            .setStage(stage)
            .build());
  }

  private boolean putNovelOperation(ExecutionStage.Value stage) throws InterruptedException {
    storage.put(simpleActionDigest.getHash(), simpleAction.toByteString());
    storage.put(simpleCommandDigest.getHash(), simpleCommand.toByteString());
    return instance.putOperation(createOperation("does-not-exist", stage));
  }

  @Test
  public void novelPutUnknownOperationReturnsTrue() throws InterruptedException {
    assertThat(putNovelOperation(UNKNOWN)).isTrue();
  }

  @Test
  public void novelPutCacheCheckOperationReturnsTrue() throws InterruptedException {
    assertThat(putNovelOperation(CACHE_CHECK)).isTrue();
  }

  @Test
  public void novelPutQueuedOperationReturnsTrue() throws InterruptedException {
    assertThat(putNovelOperation(QUEUED)).isTrue();
  }

  @Test
  public void novelPutExecutingOperationReturnsFalse() throws InterruptedException {
    assertThat(putNovelOperation(EXECUTING)).isFalse();
  }

  @Test
  public void novelPutCompletedOperationReturnsTrue() throws InterruptedException {
    assertThat(putNovelOperation(COMPLETED)).isTrue();
  }

  private Digest createAction(Action.Builder actionBuilder) {
    Command command = Command.newBuilder().addArguments("echo").build();
    ByteString commandBlob = command.toByteString();
    Digest commandDigest = DIGEST_UTIL.compute(commandBlob);
    storage.put(commandDigest.getHash(), commandBlob);

    Directory root = Directory.getDefaultInstance();
    Digest rootDigest = DIGEST_UTIL.compute(root);
    Action action =
        actionBuilder.setCommandDigest(commandDigest).setInputRootDigest(rootDigest).build();
    ByteString actionBlob = action.toByteString();
    Digest actionDigest = DIGEST_UTIL.compute(actionBlob);
    storage.put(actionDigest.getHash(), actionBlob);
    return actionDigest;
  }

  @Test
  public void actionWithExcessiveTimeoutFailsValidation()
      throws InterruptedException, InvalidProtocolBufferException {
    Duration timeout = Durations.fromSeconds(9000);
    Digest actionDigestWithExcessiveTimeout = createAction(Action.newBuilder().setTimeout(timeout));

    Watcher watcher = mock(Watcher.class);
    IllegalStateException timeoutInvalid = null;
    instance.execute(
        actionDigestWithExcessiveTimeout,
        /* skipCacheLookup=*/ true,
        ExecutionPolicy.getDefaultInstance(),
        ResultsCachePolicy.getDefaultInstance(),
        RequestMetadata.getDefaultInstance(),
        watcher);
    ArgumentCaptor<Operation> watchCaptor = ArgumentCaptor.forClass(Operation.class);
    verify(watcher, times(1)).observe(watchCaptor.capture());
    Operation watchOperation = watchCaptor.getValue();
    assertThat(watchOperation.getResponse().is(ExecuteResponse.class)).isTrue();
    Status status = watchOperation.getResponse().unpack(ExecuteResponse.class).getStatus();
    assertThat(status.getCode()).isEqualTo(Code.FAILED_PRECONDITION.getNumber());
    assertThat(status.getDetailsCount()).isEqualTo(1);
    assertThat(status.getDetails(0).is(PreconditionFailure.class)).isTrue();
    PreconditionFailure preconditionFailure =
        status.getDetails(0).unpack(PreconditionFailure.class);
    assertThat(preconditionFailure.getViolationsList())
        .contains(
            Violation.newBuilder()
                .setType(VIOLATION_TYPE_INVALID)
                .setSubject(
                    Durations.toString(timeout)
                        + " > "
                        + Durations.toString(MAXIMUM_ACTION_TIMEOUT))
                .setDescription(TIMEOUT_OUT_OF_BOUNDS)
                .build());
  }

  @Test
  public void actionWithMaximumTimeoutIsValid() throws InterruptedException {
    Digest actionDigestWithExcessiveTimeout =
        createAction(Action.newBuilder().setTimeout(MAXIMUM_ACTION_TIMEOUT));

    Watcher watcher = mock(Watcher.class);
    instance.execute(
        actionDigestWithExcessiveTimeout,
        /* skipCacheLookup=*/ true,
        ExecutionPolicy.getDefaultInstance(),
        ResultsCachePolicy.getDefaultInstance(),
        RequestMetadata.getDefaultInstance(),
        watcher);
    verify(watcher, atLeastOnce()).observe(any(Operation.class));
  }

  @Test
  public void pollFailsWithNullRequeuer() throws InterruptedException {
    Operation operation =
        Operation.newBuilder()
            .setName("test-operation")
            .setMetadata(
                Any.pack(ExecuteOperationMetadata.newBuilder().setStage(EXECUTING).build()))
            .build();

    outstandingOperations.put(operation.getName(), operation);

    assertThat(instance.pollOperation(operation.getName(), EXECUTING)).isFalse();
  }

  @Test
  public void matchCancelRemovesFromWorkers() throws InterruptedException {
    MatchListener listener = mock(MatchListener.class);
    instance.match(Platform.getDefaultInstance(), listener);
    ArgumentCaptor<Runnable> onCancelHandlerCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(listener, times(1)).setOnCancelHandler(onCancelHandlerCaptor.capture());
    onCancelHandlerCaptor.getValue().run();
    assertThat(workers).isEmpty();
  }

  @Test
  public void requeueRemovesRequeuers() throws InterruptedException {
    Digest actionDigest = createAction(Action.newBuilder());
    instance.execute(
        actionDigest,
        /* skipCacheLookup=*/ true,
        ExecutionPolicy.getDefaultInstance(),
        ResultsCachePolicy.getDefaultInstance(),
        RequestMetadata.getDefaultInstance(),
        (operation) -> {});
    MatchListener listener = mock(MatchListener.class);
    when(listener.onEntry(any(QueueEntry.class))).thenReturn(true);
    instance.match(Platform.getDefaultInstance(), listener);
    ArgumentCaptor<QueueEntry> queueEntryCaptor = ArgumentCaptor.forClass(QueueEntry.class);
    verify(listener, times(1)).onEntry(queueEntryCaptor.capture());
    QueueEntry queueEntry = queueEntryCaptor.getValue();
    assertThat(queueEntry).isNotNull();
    String operationName = queueEntry.getExecuteEntry().getOperationName();
    assertThat(requeuers).isNotEmpty();
    Operation queuedOperation = outstandingOperations.get(operationName);
    assertThat(instance.isQueued(queuedOperation)).isTrue();
    instance.putOperation(queuedOperation); // requeue
    assertThat(requeuers).isEmpty();
    assertThat(outstandingOperations.get(operationName)).isEqualTo(queuedOperation);
  }

  @Test
  public void executeRespectsSkipCacheLookup() throws InterruptedException {
    Digest actionDigest = createAction(Action.newBuilder());
    // populate actionDigest in AC
    instance.putActionResult(
        DigestUtil.asActionKey(actionDigest), ActionResult.getDefaultInstance());
    AtomicReference<Operation> operation = new AtomicReference<>(null);
    instance.execute(
        actionDigest,
        /* skipCacheLookup=*/ true,
        ExecutionPolicy.getDefaultInstance(),
        ResultsCachePolicy.getDefaultInstance(),
        RequestMetadata.getDefaultInstance(),
        o -> operation.compareAndSet(null, o));

    String operationName = operation.get().getName();
    assertThat(outstandingOperations.contains(operationName)).isTrue();
    Operation queuedOperation = outstandingOperations.get(operationName);
    assertThat(instance.isQueued(queuedOperation)).isTrue();
  }
}
