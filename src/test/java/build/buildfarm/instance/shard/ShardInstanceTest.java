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

import static com.google.common.truth.Truth.assertThat;
import static com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata.Stage.QUEUED;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.ShardBackplane;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.Instance.CommittingOutputStream;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.cache.CacheLoader;
import com.google.common.util.concurrent.Futures;
import com.google.devtools.remoteexecution.v1test.Action;
import com.google.devtools.remoteexecution.v1test.Command;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.Directory;
import com.google.longrunning.Operation;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import java.io.IOException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class ShardInstanceTest {
  private static final DigestUtil DIGEST_UTIL = new DigestUtil(HashFunction.SHA256);

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
    instance = new ShardInstance("shard", DIGEST_UTIL, mockBackplane, false, mockOnStop, mockInstanceLoader);
  }

  private Action createAction() throws Exception {
    String workerName = "worker";
    when(mockInstanceLoader.load(eq(workerName))).thenReturn(mockWorkerInstance);

    Directory inputRoot = Directory.getDefaultInstance();
    Digest inputRootDigest = DIGEST_UTIL.compute(inputRoot);
    when(mockBackplane.getTree(eq(inputRootDigest))).thenReturn(ImmutableList.of(inputRoot));
    when(mockBackplane.canQueue()).thenReturn(true);

    when(mockBackplane.getRandomWorker()).thenReturn(workerName);
    when(mockBackplane.getWorkerSet()).thenReturn(ImmutableSet.of(workerName));

    Command command = Command.newBuilder()
        .addAllArguments(ImmutableList.of("true"))
        .build();
    ByteString commandBlob = command.toByteString();
    Digest commandDigest = DIGEST_UTIL.compute(command);

    // janky - need a better supplier here
    when(mockWorkerInstance.putBlob(eq(commandBlob))).thenReturn(commandDigest);
    when(mockWorkerInstance.findMissingBlobs(eq(ImmutableList.of(commandDigest)))).thenReturn(ImmutableList.of());
    instance.putBlob(commandBlob);

    return Action.newBuilder()
        .setCommandDigest(commandDigest)
        .setInputRootDigest(DIGEST_UTIL.compute(inputRoot))
        .build();
  }

  @Test
  public void queueActionPutFailureDeletesOperation() throws Exception {
    Action action = createAction();
    Digest actionDigest = DIGEST_UTIL.compute(action);
    CommittingOutputStream mockCommittedOutputStream = mock(CommittingOutputStream.class);
    when(mockCommittedOutputStream.getCommittedFuture()).thenReturn(Futures.immediateFailedFuture(Status.UNKNOWN.asRuntimeException()));

    when(mockWorkerInstance.getStreamOutput(
        matches("^uploads/.*/blobs/" + DigestUtil.toString(actionDigest) + "$"),
        eq(actionDigest.getSizeBytes()))).thenReturn(mockCommittedOutputStream);

    Operation operation = instance.queue(action, false).get();
    instance.stop();

    verify(mockBackplane, times(1)).deleteOperation(eq(operation.getName()));
  }

  @Test
  public void queueOperationPutFailureDeletesOperation() throws Exception {
    Action action = createAction();
    Digest actionDigest = DIGEST_UTIL.compute(action);
    CommittingOutputStream mockCommittedOutputStream = mock(CommittingOutputStream.class);
    when(mockCommittedOutputStream.getCommittedFuture()).thenReturn(Futures.immediateFuture(actionDigest.getSizeBytes()));

    when(mockWorkerInstance.getStreamOutput(
        matches("^uploads/.*/blobs/" + DigestUtil.toString(actionDigest) + "$"),
        eq(actionDigest.getSizeBytes()))).thenReturn(mockCommittedOutputStream);

    when(mockBackplane.putOperation(any(Operation.class), eq(QUEUED)))
        .thenThrow(new IOException(Status.UNAVAILABLE.asRuntimeException()));

    Operation operation = instance.queue(action, false).get();
    instance.stop();

    verify(mockBackplane, times(1)).deleteOperation(eq(operation.getName()));
  }
}
