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

package build.buildfarm.server;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.instance.Instance;
import build.buildfarm.metrics.log.LogMetricsPublisher;
import build.buildfarm.server.ExecutionService.KeepaliveWatcher;
import build.buildfarm.v1test.MetricsConfig;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.longrunning.Operation;
import io.grpc.stub.ServerCallStreamObserver;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

@RunWith(JUnit4.class)
public class ExecutionServiceTest {
  private Instance instance;

  @Before
  public void setUp() throws Exception {
    instance = mock(Instance.class);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void keepaliveIsCancelledWithContext() throws Exception {
    ScheduledExecutorService keepaliveScheduler = newSingleThreadScheduledExecutor();
    ExecutionService service =
        new ExecutionService(
            instance,
            /* keepaliveAfter=*/ 1,
            /* keepaliveUnit=*/ SECONDS, // far enough in the future that we'll get scheduled and
            keepaliveScheduler,
            new LogMetricsPublisher(
                MetricsConfig.getDefaultInstance())); // cancelled without executing
    ServerCallStreamObserver<Operation> response = mock(ServerCallStreamObserver.class);
    RequestMetadata requestMetadata = RequestMetadata.newBuilder().build();
    Operation operation =
        Operation.newBuilder().setName("immediately-cancelled-watch-operation").build();
    KeepaliveWatcher watcher = service.createWatcher(response, requestMetadata);
    watcher.observe(operation);
    ListenableFuture<?> future = watcher.getFuture();
    assertThat(future).isNotNull();
    ArgumentCaptor<Runnable> onCancelHandlerCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(response, times(1)).setOnCancelHandler(onCancelHandlerCaptor.capture());
    Runnable onCancelHandler = onCancelHandlerCaptor.getValue();
    onCancelHandler.run();
    assertThat(future.isCancelled()).isTrue();
    assertThat(shutdownAndAwaitTermination(keepaliveScheduler, 1, SECONDS)).isTrue();
    // should only get one call for the real operation
    verify(response, times(1)).onNext(operation);
  }
}
