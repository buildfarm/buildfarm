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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Digest;
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.google.protobuf.Duration;
import io.grpc.StatusException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import javax.naming.ConfigurationException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class ShardWorkerContextTest {
  private final DigestUtil DIGEST_UTIL = new DigestUtil(HashFunction.SHA256);

  private Path root;

  @Mock private Backplane backplane;

  @Mock private ExecFileSystem execFileSystem;

  @Mock private InputStreamFactory inputStreamFactory;

  @Mock private Instance instance;

  @Mock private CasWriter writer;

  @Before
  public void setUp() throws ConfigurationException {
    BuildfarmConfigs.loadConfigs();
    Queue queue = new Queue();
    queue.setProperties(new ArrayList<>());
    List queues = new ArrayList();
    queues.add(queue);
    BuildfarmConfigs.getInstance().getBackplane().setQueues(queues);

    MockitoAnnotations.initMocks(this);
    when(instance.getDigestUtil()).thenReturn(DIGEST_UTIL);
    root = Iterables.getFirst(Jimfs.newFileSystem(Configuration.unix()).getRootDirectories(), null);
  }

  WorkerContext createTestContext() {
    return createTestContext(Platform.getDefaultInstance(), /* policies=*/ ImmutableList.of());
  }

  WorkerContext createTestContext(Platform platform, Iterable<ExecutionPolicy> policies) {
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
        writer);
  }

  @Test(expected = StatusException.class)
  public void outputFileIsDirectoryThrowsStatusExceptionOnUpload() throws Exception {
    Files.createDirectories(root.resolve("output"));
    WorkerContext context = createTestContext();
    context.uploadOutputs(
        Digest.getDefaultInstance(),
        ActionResult.newBuilder(),
        root,
        ImmutableList.of("output"),
        ImmutableList.of());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void queueEntryWithExecutionPolicyPlatformMatches() throws Exception {
    WorkerContext context =
        createTestContext(
            Platform.getDefaultInstance(), ImmutableList.of(new ExecutionPolicy("foo")));
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
}
