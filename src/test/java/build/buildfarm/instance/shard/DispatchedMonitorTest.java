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

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import build.buildfarm.backplane.Backplane;
import build.buildfarm.v1test.DispatchedOperation;
import build.buildfarm.v1test.ExecuteEntry;
import build.buildfarm.v1test.QueueEntry;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class DispatchedMonitorTest {
  @Mock private Backplane backplane;

  @Mock private BiFunction<QueueEntry, Duration, ListenableFuture<Void>> requeuer;

  private DispatchedMonitor dispatchedMonitor;

  @Before
  public void setUp() throws InterruptedException, IOException {
    MockitoAnnotations.initMocks(this);
    when(requeuer.apply(any(QueueEntry.class), any(Duration.class)))
        .thenReturn(immediateFailedFuture(new RuntimeException("unexpected requeue")));
    dispatchedMonitor = new DispatchedMonitor(backplane, requeuer, /* intervalSeconds=*/ 0);
  }

  @Test
  public void shouldStopWhenBackplanIsStopped() {
    when(backplane.isStopped()).thenReturn(true);

    dispatchedMonitor.run();
    verify(backplane, atLeastOnce()).isStopped();
    verifyNoInteractions(requeuer);
  }

  @Test
  public void shouldIgnoreOperationWithFutureRequeueAt() throws Exception {
    when(backplane.canQueue()).thenReturn(true);
    when(backplane.getDispatchedOperations())
        .thenReturn(
            ImmutableList.of(
                DispatchedOperation.newBuilder().setRequeueAt(Long.MAX_VALUE).build()));
    dispatchedMonitor.iterate();
    verifyNoInteractions(requeuer);
  }

  @Test
  public void shouldRequeueOperationWithEarlyRequeueAt() throws Exception {
    QueueEntry queueEntry =
        QueueEntry.newBuilder()
            .setExecuteEntry(
                ExecuteEntry.newBuilder()
                    .setOperationName("operation-with-early-requeue-at")
                    .build())
            .build();
    when(backplane.canQueue()).thenReturn(true);
    when(backplane.getDispatchedOperations())
        .thenReturn(
            ImmutableList.of(
                DispatchedOperation.newBuilder()
                    .setRequeueAt(0)
                    .setQueueEntry(queueEntry)
                    .build()));
    when(requeuer.apply(eq(queueEntry), any(Duration.class))).thenReturn(immediateFuture(null));
    dispatchedMonitor.iterate();
    verify(requeuer, times(1)).apply(queueEntry, Durations.fromSeconds(60));
  }

  @Test
  public void shouldIgnoreOperationWithEarlyRequeueAtWhenBackplaneDisallowsQueueing()
      throws Exception {
    QueueEntry queueEntry =
        QueueEntry.newBuilder()
            .setExecuteEntry(
                ExecuteEntry.newBuilder()
                    .setOperationName("operation-with-early-requeue-at")
                    .build())
            .build();
    when(backplane.canQueue()).thenReturn(false);
    when(backplane.getDispatchedOperations())
        .thenReturn(
            ImmutableList.of(
                DispatchedOperation.newBuilder()
                    .setRequeueAt(0)
                    .setQueueEntry(queueEntry)
                    .build()));
    when(requeuer.apply(eq(queueEntry), any(Duration.class))).thenReturn(immediateFuture(null));
    dispatchedMonitor.iterate();
    verifyNoInteractions(requeuer);
  }

  @Test
  public void shouldIgnoreBackplaneException() throws Exception {
    when(backplane.canQueue()).thenReturn(true);
    when(backplane.getDispatchedOperations())
        .thenThrow(new IOException("transient error condition"));
    dispatchedMonitor.iterate();
    verifyNoInteractions(requeuer);
  }

  @Test
  public void shouldIgnoreExecutionException() throws Exception {
    QueueEntry queueEntry =
        QueueEntry.newBuilder()
            .setExecuteEntry(
                ExecuteEntry.newBuilder()
                    .setOperationName("operation-with-early-requeue-at")
                    .build())
            .build();
    when(backplane.canQueue()).thenReturn(true);
    when(backplane.getDispatchedOperations())
        .thenReturn(
            ImmutableList.of(
                DispatchedOperation.newBuilder()
                    .setRequeueAt(0)
                    .setQueueEntry(queueEntry)
                    .build()));
    when(requeuer.apply(eq(queueEntry), any(Duration.class)))
        .thenReturn(immediateFailedFuture(new Exception("error during requeue")));
    dispatchedMonitor.iterate();
    verify(requeuer, times(1)).apply(queueEntry, Durations.fromSeconds(60));
  }

  @Test
  public void shouldStopOnInterrupt() throws IOException, InterruptedException {
    when(backplane.canQueue()).thenReturn(true);
    when(backplane.getDispatchedOperations()).thenReturn(ImmutableList.of());
    AtomicBoolean readyForInterrupt = new AtomicBoolean(false);
    doAnswer(
            (invocation) -> {
              readyForInterrupt.set(true);
              return ImmutableList.of();
            })
        .when(backplane)
        .getDispatchedOperations();

    Thread thread = new Thread(dispatchedMonitor);
    thread.start();
    while (!readyForInterrupt.get()) {
      TimeUnit.MICROSECONDS.sleep(1);
    }
    thread.interrupt();
    thread.join();
  }

  @Test
  public void shouldIterateUntilBackplaneIsStopped() throws IOException {
    when(backplane.canQueue()).thenReturn(true);
    when(backplane.getDispatchedOperations()).thenReturn(ImmutableList.of());
    when(backplane.isStopped()).thenReturn(false).thenReturn(true);
    dispatchedMonitor.run();
    verify(backplane, atLeastOnce()).getDispatchedOperations();
    verify(backplane, times(2)).isStopped();
  }

  @Test(expected = RuntimeException.class)
  public void getOnlyInterruptiblyShouldPropagateRuntimeExceptions() throws InterruptedException {
    DispatchedMonitor.getOnlyInterruptibly(
        immediateFailedFuture(new RuntimeException("spurious exception")));
  }

  @Test(expected = UncheckedExecutionException.class)
  public void getOnlyInterruptiblyShouldWrapCheckedExceptions() throws InterruptedException {
    DispatchedMonitor.getOnlyInterruptibly(
        immediateFailedFuture(new IOException("spurious io exception")));
  }
}
