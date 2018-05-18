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
import static org.mockito.Mockito.when;

import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.cas.ContentAddressableStorage.Blob;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.instance.Instance;
import build.buildfarm.v1test.ActionCacheConfig;
import build.buildfarm.v1test.DelegateCASConfig;
import build.buildfarm.v1test.MemoryInstanceConfig;
import com.google.common.collect.ImmutableList;
import com.google.devtools.remoteexecution.v1test.Action;
import com.google.devtools.remoteexecution.v1test.ActionResult;
import com.google.longrunning.Operation;
import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class MemoryInstanceTest {
  private Instance instance;

  private MemoryInstance.OutstandingOperations outstandingOperations;

  @Mock
  private ContentAddressableStorage storage;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    outstandingOperations = new MemoryInstance.OutstandingOperations();
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
  public void listOperationsForOutstandingOperations() {
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
  public void listOperationsLimitsPages() {
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
}
