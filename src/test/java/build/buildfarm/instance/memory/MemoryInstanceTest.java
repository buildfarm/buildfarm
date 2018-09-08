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

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.cas.ContentAddressableStorage.Blob;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.OperationsMap;
import build.buildfarm.v1test.ActionCacheConfig;
import build.buildfarm.v1test.DelegateCASConfig;
import build.buildfarm.v1test.MemoryInstanceConfig;
import com.google.common.collect.ImmutableList;
import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionResult;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
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
public class MemoryInstanceTest {
  private Instance instance;

  private OperationsMap outstandingOperations;
  private Map<String, List<Predicate<Operation>>> watchers;

  @Mock
  private ContentAddressableStorage storage;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    outstandingOperations = new MemoryInstance.OutstandingOperations();
    watchers = new HashMap<>();
    MemoryInstanceConfig memoryInstanceConfig = MemoryInstanceConfig.newBuilder()
        .setListOperationsDefaultPageSize(1024)
        .setListOperationsMaxPageSize(16384)
        .setTreeDefaultPageSize(1024)
        .setTreeMaxPageSize(16384)
        .setOperationPollTimeout(Durations.fromSeconds(10))
        .setOperationCompletedDelay(Durations.fromSeconds(10))
        .setDefaultActionTimeout(Durations.fromSeconds(600))
        .setMaximumActionTimeout(Durations.fromSeconds(3600))
        .setActionCacheConfig(ActionCacheConfig.newBuilder()
            .setDelegateCas(DelegateCASConfig.getDefaultInstance())
            .build())
        .build();

    instance = new MemoryInstance(
        "memory",
        new DigestUtil(DigestUtil.HashFunction.SHA256),
        memoryInstanceConfig,
        storage,
        watchers,
        outstandingOperations);
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
    when(storage.get(instance.getDigestUtil().compute(result)))
        .thenReturn(new Blob(result.toByteString(), instance.getDigestUtil()));

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

    List<Predicate<Operation>> operationWatchers = new ArrayList<>();
    watchers.put(operation.getName(), operationWatchers);

    Predicate<Operation> watcher = (o) -> true;
    assertThat(instance.watchOperation(
        operation.getName(),
        watcher)).isTrue();
    assertThat(operationWatchers).containsExactly(watcher);
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
}
