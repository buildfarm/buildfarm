// Copyright 2019 The Bazel Authors. All rights reserved.
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

package build.buildfarm.worker.shard;

import static build.buildfarm.common.config.Server.INSTANCE_TYPE.SHARD;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.Platform;
import build.bazel.remote.execution.v2.Platform.Property;
import build.buildfarm.backplane.Backplane;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.common.config.BuildfarmConfigs;
import build.buildfarm.common.config.ExecutionPolicy;
import build.buildfarm.common.config.Queue;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.MatchListener;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.worker.WorkerContext;
import build.buildfarm.worker.resources.LocalResourceSet;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class ShardWorkerContextTest {
  private final DigestUtil DIGEST_UTIL = new DigestUtil(HashFunction.SHA256);

  private BuildfarmConfigs configs = BuildfarmConfigs.getInstance();

  @Mock private Backplane backplane;

  @Mock private ExecFileSystem execFileSystem;

  @Mock private InputStreamFactory inputStreamFactory;

  @Mock private Instance instance;

  @Mock private CasWriter writer;

  @Before
  public void setUp() throws Exception {
    configs.getServer().setInstanceType(SHARD);
    configs.getServer().setName("shard");
    configs.getWorker().setPublicName("localhost:8981");
    configs.getBackplane().setRedisUri("redis://localhost:6379");
    Queue queue = new Queue();
    queue.setProperties(new ArrayList<>());
    Queue[] queues = new Queue[1];
    queues[0] = queue;
    configs.getBackplane().setQueues(queues);

    MockitoAnnotations.initMocks(this);
    when(instance.getDigestUtil()).thenReturn(DIGEST_UTIL);
  }

  WorkerContext createTestContext() {
    return createTestContext(/* policies=*/ ImmutableList.of());
  }

  WorkerContext createTestContext(Iterable<ExecutionPolicy> policies) {
    return new ShardWorkerContext(
        "test",
        /* operationPollPeriod=*/ Duration.getDefaultInstance(),
        /* operationPoller=*/ (queueEntry, stage, requeueAt) -> false,
        /* inlineContentLimit=*/
        /* inputFetchStageWidth=*/ 0,
        /* executeStageWidth=*/ 0,
        /* inputFetchDeadline=*/ 60,
        backplane,
        execFileSystem,
        inputStreamFactory,
        policies,
        instance,
        /* deadlineAfter=*/
        /* deadlineAfterUnits=*/
        /* defaultActionTimeout=*/ Duration.getDefaultInstance(),
        /* maximumActionTimeout=*/ Duration.getDefaultInstance(),
        /* defaultMaxCores=*/ 0,
        /* limitGlobalExecution=*/ false,
        /* onlyMulticoreTests=*/ false,
        /* allowBringYourOwnContainer=*/ false,
        /* errorOperationRemainingResources=*/ false,
        /* resourceSet=*/ new LocalResourceSet(),
        writer);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void queueEntryWithExecutionPolicyPlatformMatches() throws Exception {
    WorkerContext context = createTestContext(ImmutableList.of(new ExecutionPolicy("foo")));
    Platform matchPlatform =
        Platform.newBuilder()
            .addProperties(
                Property.newBuilder().setName("execution-policy").setValue("foo").build())
            .build();
    QueueEntry queueEntry = QueueEntry.newBuilder().setPlatform(matchPlatform).build();
    when(backplane.dispatchOperation(any(List.class)))
        .thenReturn(queueEntry)
        .thenReturn(null); // provide a match completion in failure case
    MatchListener listener = mock(MatchListener.class);
    context.match(listener);
    verify(listener, times(1)).onEntry(queueEntry);
  }

  @Test
  public void dequeueMatchSettingsPlatformRejectsInvalidQueueEntry() throws Exception {
    configs.getWorker().getDequeueMatchSettings().setAcceptEverything(false);
    configs.getWorker().getDequeueMatchSettings().setAllowUnmatched(false);
    WorkerContext context = createTestContext();
    Platform matchPlatform =
        Platform.newBuilder()
            .addProperties(Property.newBuilder().setName("os").setValue("randos").build())
            .build();
    QueueEntry queueEntry = QueueEntry.newBuilder().setPlatform(matchPlatform).build();
    when(backplane.dispatchOperation(any(List.class)))
        .thenReturn(queueEntry)
        .thenReturn(null); // provide a match completion in failure case
    MatchListener listener = mock(MatchListener.class);
    context.match(listener);
    verify(listener, never()).onEntry(queueEntry);
  }

  @Test
  public void dequeueMatchSettingsPlatformAcceptsValidQueueEntry() throws Exception {
    configs.getWorker().getDequeueMatchSettings().setAcceptEverything(false);
    configs.getWorker().getDequeueMatchSettings().setAllowUnmatched(false);
    Platform testOSPlatform =
        Platform.newBuilder()
            .addProperties(Property.newBuilder().setName("os").setValue("test").build())
            .build();
    configs.getWorker().getDequeueMatchSettings().setPlatform(testOSPlatform);
    WorkerContext context = createTestContext();
    QueueEntry queueEntry = QueueEntry.newBuilder().setPlatform(testOSPlatform).build();
    when(backplane.dispatchOperation(any(List.class)))
        .thenReturn(queueEntry)
        .thenReturn(null); // provide a match completion in failure case
    MatchListener listener = mock(MatchListener.class);
    context.match(listener);
    verify(listener, times(1)).onEntry(queueEntry);
  }
}
