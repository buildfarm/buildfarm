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
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import build.buildfarm.common.ContentAddressableStorage;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.ShardBackplane;
import build.buildfarm.instance.Instance.MatchListener;
import build.buildfarm.v1test.ShardWorkerInstanceConfig;
import build.buildfarm.worker.InputStreamFactory;
import com.google.longrunning.Operation;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import javax.naming.ConfigurationException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class ShardWorkerInstanceTest {
  private final DigestUtil DIGEST_UTIL = new DigestUtil(HashFunction.SHA256);

  @Mock
  private ShardBackplane backplane;

  @Mock
  private ContentAddressableStorage storage;

  @Mock
  private InputStreamFactory inputStreamFactory;

  private ShardWorkerInstance instance;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    instance = new ShardWorkerInstance(
        "test",
        DIGEST_UTIL,
        backplane,
        storage,
        inputStreamFactory,
        ShardWorkerInstanceConfig.getDefaultInstance());
  }

  @Test(expected = SocketException.class)
  public void dispatchOperationThrowsOnSocketException() throws IOException, InterruptedException {
    when(backplane.dispatchOperation())
        .thenThrow(SocketException.class);
    MatchListener listener = mock(MatchListener.class);
    instance.dispatchOperation(listener);
  }

  @Test
  public void dispatchOperationIgnoresNull() throws IOException, InterruptedException {
    when(backplane.dispatchOperation())
        .thenReturn(null)
        .thenReturn("op");
    MatchListener listener = mock(MatchListener.class);
    assertThat(instance.dispatchOperation(listener)).isEqualTo("op");
    verify(backplane, times(2)).dispatchOperation();
  }

  @Test(expected = StatusRuntimeException.class)
  public void getOperationThrowsOnSocketException() throws IOException, InterruptedException {
    when(backplane.getOperation(any(String.class)))
        .thenThrow(SocketException.class);
    instance.getOperation("op");
  }
}
