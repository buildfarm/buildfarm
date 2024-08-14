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

package build.buildfarm.worker.shard;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.backplane.Backplane;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.instance.MatchListener;
import build.buildfarm.v1test.ExecuteEntry;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.Tree;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.longrunning.Operation;
import com.google.protobuf.ByteString;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.net.SocketException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class WorkerInstanceTest {
  private final DigestUtil DIGEST_UTIL = new DigestUtil(HashFunction.SHA256);

  @Mock private Backplane backplane;

  @Mock private ContentAddressableStorage storage;

  private WorkerInstance instance;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    instance = new WorkerInstance("test", DIGEST_UTIL, backplane, storage);
  }

  @SuppressWarnings("unchecked")
  @Test(expected = SocketException.class)
  public void dispatchOperationThrowsOnSocketException() throws IOException, InterruptedException {
    when(backplane.dispatchOperation(any(List.class))).thenThrow(SocketException.class);
    MatchListener listener = mock(MatchListener.class);
    instance.dispatchOperation(listener);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void dispatchOperationIgnoresNull() throws IOException, InterruptedException {
    QueueEntry queueEntry =
        QueueEntry.newBuilder()
            .setExecuteEntry(ExecuteEntry.newBuilder().setOperationName("op"))
            .build();
    when(backplane.dispatchOperation(any(List.class))).thenReturn(null).thenReturn(queueEntry);
    MatchListener listener = mock(MatchListener.class);
    assertThat(instance.dispatchOperation(listener)).isEqualTo(queueEntry);
    verify(backplane, times(2)).dispatchOperation(any(List.class));
  }

  @Test(expected = StatusRuntimeException.class)
  public void getOperationThrowsOnSocketException() throws IOException, InterruptedException {
    when(backplane.getOperation(any(String.class))).thenThrow(SocketException.class);
    instance.getOperation("op");
  }

  @Test(expected = UnsupportedOperationException.class)
  public void getActionResultIsUnsupported() throws InterruptedException {
    try {
      instance.getActionResult(null, null).get();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      Throwables.propagateIfInstanceOf(cause, RuntimeException.class);
      throw new RuntimeException(cause);
    }
  }

  @Test
  public void putActionResultDelegatesToBackplane() throws IOException {
    DigestUtil.ActionKey key =
        DigestUtil.asActionKey(DIGEST_UTIL.compute(ByteString.copyFromUtf8("Hello, World")));
    ActionResult result = ActionResult.getDefaultInstance();
    instance.putActionResult(key, result);
    verify(backplane, times(1)).putActionResult(key, result);
  }

  @Test
  public void listOperationsIsUnsupported() {
    ImmutableList.Builder<Operation> operations = new ImmutableList.Builder<>();
    instance.listOperations(
        /* pageSize=*/ 0, /* pageToken=*/ "", /* filter=*/ "", /* operations=*/ operations);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void readResourceNameIsUnsupported() {
    instance.readResourceName(Compressor.Value.IDENTITY, null);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void getTreeIsUnsupported() {
    instance.getTree(
        /* rootDigest=*/ Digest.getDefaultInstance(),
        /* pageSize=*/ 0,
        /* pageToken=*/ "",
        /* tree=*/ Tree.newBuilder());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void getOperationStreamWriteIsUnsupported() {
    instance.getOperationStreamWrite(/* name=*/ null);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void newOperationStreamInputIsUnsupported() {
    instance.newOperationStreamInput(
        /* name=*/ null,
        /* offset=*/ 0,
        /* deadlineAfter=*/
        /* deadlineAfterUnits=*/ RequestMetadata.getDefaultInstance());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void executeIsUnsupported() {
    instance.execute(
        /* actionDigest=*/ null,
        /* skipCacheLookup=*/ false,
        /* executionPolicy=*/ null,
        /* resultsCachePolicy=*/ null,
        /* requestMetadata=*/ null,
        /* watcher=*/ null);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void matchIsUnsupported() throws InterruptedException {
    instance.match(/* platform=*/ null, /* listener=*/ null);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void cancelOperationIsUnsupported() throws InterruptedException {
    instance.cancelOperation(/* name=*/ null);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void deleteOperation() throws InterruptedException {
    instance.deleteOperation(/* name=*/ null);
  }
}
