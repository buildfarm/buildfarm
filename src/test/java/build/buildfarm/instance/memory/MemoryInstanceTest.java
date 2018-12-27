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

import static build.bazel.remote.execution.v2.ExecuteOperationMetadata.Stage.CACHE_CHECK;
import static build.bazel.remote.execution.v2.ExecuteOperationMetadata.Stage.COMPLETED;
import static build.bazel.remote.execution.v2.ExecuteOperationMetadata.Stage.EXECUTING;
import static build.bazel.remote.execution.v2.ExecuteOperationMetadata.Stage.QUEUED;
import static build.bazel.remote.execution.v2.ExecuteOperationMetadata.Stage.UNKNOWN;
import static build.buildfarm.instance.AbstractServerInstance.INVALID_DIGEST;
import static build.buildfarm.cas.ContentAddressableStorages.casMapDecorator;
import static com.google.common.collect.Multimaps.synchronizedSetMultimap;
import static com.google.common.truth.Truth.assertThat;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata.Stage;
import build.bazel.remote.execution.v2.ExecutionPolicy;
import build.bazel.remote.execution.v2.ResultsCachePolicy;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.cas.ContentAddressableStorage.Blob;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.instance.OperationsMap;
import build.buildfarm.v1test.ActionCacheConfig;
import build.buildfarm.v1test.DelegateCASConfig;
import build.buildfarm.v1test.MemoryInstanceConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;
import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionResult;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Durations;
import com.google.rpc.Code;
import com.google.rpc.Status;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

@RunWith(JUnit4.class)
public class MemoryInstanceTest {
  private final DigestUtil DIGEST_UTIL = new DigestUtil(DigestUtil.HashFunction.SHA256);
  private final Command simpleCommand = Command.newBuilder()
      .addArguments("true")
      .build();
  private final Digest simpleCommandDigest =
      DIGEST_UTIL.compute(simpleCommand);
  private final Action simpleAction = Action.newBuilder()
      .setCommandDigest(simpleCommandDigest)
      .build();
  private final Digest simpleActionDigest = DIGEST_UTIL.compute(simpleAction);
  private final Duration MAXIMUM_ACTION_TIMEOUT = Durations.fromSeconds(3600);

  private MemoryInstance instance;

  private OperationsMap outstandingOperations;
  private SetMultimap<String, Predicate<Operation>> watchers;
  private ExecutorService watchersThreadPool;

  private Map<Digest, ByteString> storage;

  @Before
  public void setUp() throws Exception {
    outstandingOperations = new MemoryInstance.OutstandingOperations();
    watchers = synchronizedSetMultimap(
        MultimapBuilder
            .hashKeys()
            .hashSetValues(/* expectedValuesPerKey=*/ 1)
            .build());
    watchersThreadPool = newFixedThreadPool(1);
    MemoryInstanceConfig memoryInstanceConfig = MemoryInstanceConfig.newBuilder()
        .setListOperationsDefaultPageSize(1024)
        .setListOperationsMaxPageSize(16384)
        .setTreeDefaultPageSize(1024)
        .setTreeMaxPageSize(16384)
        .setOperationPollTimeout(Durations.fromSeconds(10))
        .setOperationCompletedDelay(Durations.fromSeconds(10))
        .setDefaultActionTimeout(Durations.fromSeconds(600))
        .setMaximumActionTimeout(MAXIMUM_ACTION_TIMEOUT)
        .setActionCacheConfig(ActionCacheConfig.newBuilder()
            .setDelegateCas(DelegateCASConfig.getDefaultInstance())
            .build())
        .build();

    storage = Maps.newHashMap();
    instance = new MemoryInstance(
        "memory",
        DIGEST_UTIL,
        memoryInstanceConfig,
        casMapDecorator(storage),
        watchers,
        watchersThreadPool,
        outstandingOperations);
  }

  @After
  public void tearDown() throws Exception {
    watchersThreadPool.shutdown();
    watchersThreadPool.awaitTermination(Long.MAX_VALUE, NANOSECONDS);
  }

  @Test
  public void listOperationsForEmptyOutstanding() {
    ImmutableList.Builder<Operation> operations = new ImmutableList.Builder<>();

    String nextToken = instance.listOperations(
        /* pageSize=*/ 1,
        /* pageToken=*/ "",
        /* filter=*/ "",
        operations);
    assertThat(nextToken).isEqualTo("");
    assertThat(operations.build()).isEmpty();
  }

  @Test
  public void listOperationsForOutstandingOperations() throws InterruptedException {
    Operation operation = Operation.newBuilder()
        .setName("test-operation")
        .build();

    outstandingOperations.put(operation.getName(), operation);

    ImmutableList.Builder<Operation> operations = new ImmutableList.Builder<>();

    String nextToken = instance.listOperations(
        /* pageSize=*/ 1,
        /* pageToken=*/ "",
        /* filter=*/ "",
        operations);
    // we should have reached the end of list
    assertThat(nextToken).isEqualTo("");
    assertThat(operations.build()).containsExactly(operation);
  }

  @Test
  public void listOperationsLimitsPages() throws InterruptedException {
    Operation testOperation1 = Operation.newBuilder()
        .setName("test-operation1")
        .build();

    Operation testOperation2 = Operation.newBuilder()
        .setName("test-operation2")
        .build();

    outstandingOperations.put(testOperation1.getName(), testOperation1);
    outstandingOperations.put(testOperation2.getName(), testOperation2);

    ImmutableList.Builder<Operation> operations = new ImmutableList.Builder<>();

    String nextToken = instance.listOperations(
        /* pageSize=*/ 1,
        /* pageToken=*/ "",
        /* filter=*/ "",
        operations);
    // we should not be at the end
    assertThat(nextToken).isNotEqualTo("");
    assertThat(operations.build().size()).isEqualTo(1);

    nextToken = instance.listOperations(
        /* pageSize=*/ 1,
        /* pageToken=*/ nextToken,
        /* filter=*/ "",
        operations);
    // we should have reached the end
    assertThat(nextToken).isEqualTo("");
    assertThat(operations.build()).containsExactly(testOperation1, testOperation2);
  }

  @Test
  public void actionCacheMissResult() {
    Action action = Action.getDefaultInstance();

    assertThat(instance.getActionResult(
        instance.getDigestUtil().computeActionKey(action))).isNull();
  }

  @Test
  public void actionCacheRetrievableByActionKey() throws InterruptedException {
    ActionResult result = ActionResult.getDefaultInstance();
    storage.put(DIGEST_UTIL.compute(result), result.toByteString());

    Action action = Action.getDefaultInstance();
    instance.putActionResult(
        instance.getDigestUtil().computeActionKey(action),
        result);
    assertThat(instance.getActionResult(
        instance.getDigestUtil().computeActionKey(action))).isEqualTo(result);
  }

  @Test
  public void missingOperationWatchInvertsWatcher() {
    Predicate<Operation> watcher = (Predicate<Operation>) mock(Predicate.class);
    when(watcher.test(eq(null))).thenReturn(true);
    assertThat(instance.watchOperation(
        "does-not-exist",
        watcher)).isFalse();
    verify(watcher, times(1)).test(eq(null));
  }

  @Test
  public void watchWithCompletedSignalsWatching() {
    Predicate<Operation> watcher = (Predicate<Operation>) mock(Predicate.class);
    when(watcher.test(eq(null))).thenReturn(false);
    assertThat(instance.watchOperation(
        "does-not-exist",
        watcher)).isTrue();
    verify(watcher, times(1)).test(eq(null));
  }

  @Test
  public void watchOperationAddsWatcher() throws InterruptedException {
    Operation operation = Operation.newBuilder()
        .setName("my-watched-operation")
        .build();
    outstandingOperations.put(operation.getName(), operation);

    Predicate<Operation> watcher = (o) -> true;
    assertThat(instance.watchOperation(
        operation.getName(),
        watcher)).isTrue();
    assertThat(watchers.get(operation.getName())).containsExactly(watcher);
  }

  @Test
  public void watchOpRaceLossInvertsTestOnInitial() throws InterruptedException {
    Operation operation = Operation.newBuilder()
        .setName("my-watched-operation")
        .build();
    Operation doneOperation = operation.toBuilder()
        .setDone(true)
        .build();

    // as a result of the initial verified test, change the operation to done
    Answer<Boolean> initialAnswer = new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        outstandingOperations.put(operation.getName(), doneOperation);
        return true;
      }
    };
    Predicate<Operation> watcher = (Predicate<Operation>) mock(Predicate.class);
    when(watcher.test(eq(operation))).thenAnswer(initialAnswer);
    when(watcher.test(eq(doneOperation))).thenReturn(false);

    // set for each verification
    outstandingOperations.put(operation.getName(), operation);

    assertThat(instance.watchOperation(
        operation.getName(),
        watcher)).isTrue();
    verify(watcher, times(1)).test(eq(operation));
    verify(watcher, times(1)).test(eq(doneOperation));

    // reset test
    outstandingOperations.remove(operation.getName());

    Predicate<Operation> unfazedWatcher = (Predicate<Operation>) mock(Predicate.class);
    when(unfazedWatcher.test(eq(operation))).thenAnswer(initialAnswer);
    // unfazed watcher is not bothered by the operation's change of done
    when(unfazedWatcher.test(eq(doneOperation))).thenReturn(true);

    // set for each verification
    outstandingOperations.put(operation.getName(), operation);
    assertThat(instance.watchOperation(
        operation.getName(),
        unfazedWatcher)).isFalse();
    verify(unfazedWatcher, times(1)).test(eq(operation));
    verify(unfazedWatcher, times(1)).test(eq(doneOperation));
  }

  @Test
  public void requeueFailOnInvalid() throws InterruptedException, InvalidProtocolBufferException {
    // These new operations are invalid as they're missing content.
    Operation queuedOperation = createOperation("my-queued-operation", QUEUED);
    outstandingOperations.put(queuedOperation.getName(), queuedOperation);

    AtomicInteger code = new AtomicInteger(Code.OK.getNumber());
    watchers.put(queuedOperation.getName(), new Predicate<Operation>() {
      @Override
      public boolean test(Operation operation) {
        code.set(operation.getError().getCode());
        return false;
      }
    });

    instance.requeueOperation(queuedOperation);

    boolean done = false;
    int waits = 0;
    while (!done) {
      assertThat(waits < 10).isTrue();
      MICROSECONDS.sleep(10);
      waits++;
      synchronized (watchers) {
        if (!watchers.containsKey(queuedOperation.getName())) {
          done = true;
        }
      }
    }
    assertThat(code.get()).isEqualTo(Code.FAILED_PRECONDITION.getNumber());
  }

  @Test
  public void requeueSucceedsOnQueued() throws InterruptedException, InvalidProtocolBufferException {
    Operation queuedOperation = createOperation("my-queued-operation", QUEUED);
    outstandingOperations.put(queuedOperation.getName(), queuedOperation);

    instance.requeueOperation(queuedOperation);
  }

  @Test
  public void requeueFailureNotifiesWatchers() throws InterruptedException {
    ExecuteOperationMetadata metadata = ExecuteOperationMetadata.newBuilder()
        .setStage(QUEUED)
        .build();
    Operation queuedOperation = createOperation("my-queued-operation", metadata);
    outstandingOperations.put(queuedOperation.getName(), queuedOperation);
    Operation erroredOperation = queuedOperation.toBuilder()
        .setDone(true)
        .setMetadata(Any.pack(metadata.toBuilder()
            .setStage(COMPLETED)
            .build()))
        .setError(Status.newBuilder()
            .setCode(Code.FAILED_PRECONDITION.getNumber())
            .setMessage(INVALID_DIGEST)
            .build())
        .build();
    Predicate<Operation> watcher = mock(Predicate.class);
    when(watcher.test(eq(queuedOperation))).thenReturn(true);
    when(watcher.test(eq(erroredOperation))).thenReturn(false);
    assertThat(instance.watchOperation(queuedOperation.getName(), watcher)).isTrue();
    assertThat(instance.requeueOperation(queuedOperation)).isFalse();

    boolean done = false;
    int waits = 0;
    while (!done) {
      assertThat(waits < 10).isTrue();
      MICROSECONDS.sleep(10);
      waits++;
      synchronized (watchers) {
        if (!watchers.containsKey(queuedOperation.getName())) {
          done = true;
        }
      }
    }

    verify(watcher, times(1)).test(eq(queuedOperation));
    verify(watcher, times(1)).test(eq(erroredOperation));
  }

  private Operation createOperation(String name, ExecuteOperationMetadata metadata) {
    return Operation.newBuilder()
        .setName(name)
        .setMetadata(Any.pack(metadata))
        .build();
  }

  private Operation createOperation(String name, Stage stage) {
    return createOperation(
        name,
        ExecuteOperationMetadata.newBuilder()
            .setActionDigest(simpleActionDigest)
            .setStage(stage)
            .build());
  }

  private boolean putNovelOperation(Stage stage) throws InterruptedException {
    storage.put(simpleActionDigest, simpleAction.toByteString());
    storage.put(simpleCommandDigest, simpleCommand.toByteString());
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
    Command command = Command.newBuilder()
        .addArguments("echo")
        .build();
    ByteString commandBlob = command.toByteString();
    Digest commandDigest = DIGEST_UTIL.compute(commandBlob);
    storage.put(commandDigest, commandBlob);

    Directory root = Directory.getDefaultInstance();
    Digest rootDigest = DIGEST_UTIL.compute(root);
    Action action = actionBuilder
        .setCommandDigest(commandDigest)
        .setInputRootDigest(rootDigest)
        .build();
    ByteString actionBlob = action.toByteString();
    Digest actionDigest = DIGEST_UTIL.compute(actionBlob);
    storage.put(actionDigest, actionBlob);
    return actionDigest;
  }

  @Test
  public void actionWithExcessiveTimeoutFailsValidation()
      throws InterruptedException {
    Duration timeout = Durations.fromSeconds(9000);
    Digest actionDigestWithExcessiveTimeout = createAction(Action.newBuilder()
        .setTimeout(timeout));

    Predicate watcher = mock(Predicate.class);
    IllegalStateException timeoutInvalid = null;
    try {
      instance.execute(
          actionDigestWithExcessiveTimeout,
          /* skipCacheLookup=*/ true,
          ExecutionPolicy.getDefaultInstance(),
          ResultsCachePolicy.getDefaultInstance(),
          watcher);
    } catch (IllegalStateException e) {
      assertThat(e.getMessage()).isEqualTo(
          String.format(
              "action timeout %s exceeds the maximum timeout %s",
              Durations.toString(timeout),
              Durations.toString(MAXIMUM_ACTION_TIMEOUT)));
      timeoutInvalid = e;
    }
    verifyZeroInteractions(watcher);
  }

  @Test
  public void actionWithMaximumTimeoutIsValid()
      throws InterruptedException {
    Digest actionDigestWithExcessiveTimeout = createAction(Action.newBuilder()
        .setTimeout(MAXIMUM_ACTION_TIMEOUT));

    Predicate watcher = mock(Predicate.class);
    when(watcher.test(any(Operation.class))).thenReturn(true);
    instance.execute(
        actionDigestWithExcessiveTimeout,
        /* skipCacheLookup=*/ true,
        ExecutionPolicy.getDefaultInstance(),
        ResultsCachePolicy.getDefaultInstance(),
        watcher);
    verify(watcher, atLeast(1)).test(any(Operation.class));
  }

  @Test
  public void pollFailsWithNullRequeuer() throws InterruptedException {
    Operation operation = Operation.newBuilder()
        .setName("test-operation")
        .setMetadata(Any.pack(
            ExecuteOperationMetadata.newBuilder()
                .setStage(Stage.EXECUTING)
                .build()))
        .build();

    outstandingOperations.put(operation.getName(), operation);

    assertThat(instance.pollOperation(
        operation.getName(),
        Stage.EXECUTING)).isFalse();
  }
}
