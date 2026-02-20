// Copyright 2018 The Buildfarm Authors. All rights reserved.
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

import static build.buildfarm.common.Scannable.SENTINEL_PAGE_TOKEN;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import build.buildfarm.common.Scannable;
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
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class DispatchedMonitorTest {
  @Mock private BiFunction<QueueEntry, Duration, ListenableFuture<Void>> requeuer;

  @Before
  public void setUp() throws InterruptedException, IOException {
    MockitoAnnotations.initMocks(this);
    when(requeuer.apply(any(QueueEntry.class), any(Duration.class)))
        .thenReturn(immediateFailedFuture(new RuntimeException("unexpected requeue")));
  }

  @Test
  public void shouldStopStopsMonitor() {
    Scannable<DispatchedOperation> location = mock(Scannable.class);
    DispatchedMonitor dispatchedMonitor =
        new DispatchedMonitor(
            /* shouldStop= */ () -> true,
            location,
            requeuer,
            /* intervalSeconds= */ 0,
            /* requeueDelay= */ Durations.fromSeconds(1));

    dispatchedMonitor.run();
    verifyNoInteractions(location);
    verifyNoInteractions(requeuer);
  }

  private static final class IterableScannable implements Scannable<DispatchedOperation> {
    private final Iterable<DispatchedOperation> dispatchedOperations;

    IterableScannable(Iterable<DispatchedOperation> dispatchedOperations) {
      this.dispatchedOperations = dispatchedOperations;
    }

    @Override
    public String getName() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String scan(int limit, String pageToken, Consumer<DispatchedOperation> onItem) {
      checkState(limit > 0);
      checkState(pageToken.equals(SENTINEL_PAGE_TOKEN));
      dispatchedOperations.forEach(onItem);
      return SENTINEL_PAGE_TOKEN;
    }
  }

  @Test
  public void shouldIgnoreOperationWithFutureRequeueAt() throws Exception {
    Iterable<DispatchedOperation> dispatchedOperations =
        ImmutableList.of(DispatchedOperation.newBuilder().setRequeueAt(Long.MAX_VALUE).build());
    Scannable<DispatchedOperation> location = new IterableScannable(dispatchedOperations);
    DispatchedMonitor dispatchedMonitor =
        new DispatchedMonitor(
            /* shouldStop= */ () -> false,
            location,
            requeuer,
            /* intervalSeconds= */ 0,
            /* requeueDelay= */ Durations.fromSeconds(1));
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
    Iterable<DispatchedOperation> dispatchedOperations =
        ImmutableList.of(
            DispatchedOperation.newBuilder().setRequeueAt(0).setQueueEntry(queueEntry).build());
    Scannable<DispatchedOperation> location = new IterableScannable(dispatchedOperations);
    when(requeuer.apply(eq(queueEntry), any(Duration.class))).thenReturn(immediateFuture(null));
    DispatchedMonitor dispatchedMonitor =
        new DispatchedMonitor(
            /* shouldStop= */ () -> false,
            location,
            requeuer,
            /* intervalSeconds= */ 0,
            /* requeueDelay= */ Durations.fromSeconds(1));
    dispatchedMonitor.iterate();
    verify(requeuer, times(1)).apply(queueEntry, Durations.fromSeconds(1));
  }

  @Test
  public void shouldIgnoreScanException() throws Exception {
    Scannable<DispatchedOperation> location = mock(Scannable.class);
    when(location.scan(any(Integer.class), any(String.class), any(Consumer.class)))
        .thenThrow(new IOException("transient error condition"));
    DispatchedMonitor dispatchedMonitor =
        new DispatchedMonitor(
            /* shouldStop= */ () -> false,
            location,
            requeuer,
            /* intervalSeconds= */ 0,
            /* requeueDelay= */ Durations.fromSeconds(1));
    dispatchedMonitor.iterate();
    verifyNoInteractions(requeuer);
    verify(location, times(1)).scan(any(Integer.class), any(String.class), any(Consumer.class));
    verifyNoMoreInteractions(location);
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
    Iterable<DispatchedOperation> dispatchedOperations =
        ImmutableList.of(
            DispatchedOperation.newBuilder().setRequeueAt(0).setQueueEntry(queueEntry).build());
    Scannable<DispatchedOperation> location = new IterableScannable(dispatchedOperations);
    when(requeuer.apply(eq(queueEntry), any(Duration.class)))
        .thenReturn(immediateFailedFuture(new Exception("error during requeue")));
    DispatchedMonitor dispatchedMonitor =
        new DispatchedMonitor(
            /* shouldStop= */ () -> false,
            location,
            requeuer,
            /* intervalSeconds= */ 0,
            /* requeueDelay= */ Durations.fromSeconds(1));
    dispatchedMonitor.iterate();
    verify(requeuer, times(1)).apply(queueEntry, Durations.fromSeconds(1));
  }

  @Test
  public void shouldStopOnInterrupt() throws IOException, InterruptedException {
    AtomicBoolean readyForInterrupt = new AtomicBoolean(false);
    Scannable<DispatchedOperation> location = mock(Scannable.class);
    doAnswer(
            (invocation) -> {
              readyForInterrupt.set(true);
              return SENTINEL_PAGE_TOKEN;
            })
        .when(location)
        .scan(any(Integer.class), any(String.class), any(Consumer.class));

    DispatchedMonitor dispatchedMonitor =
        new DispatchedMonitor(
            /* shouldStop= */ () -> false,
            location,
            requeuer,
            /* intervalSeconds= */ 0,
            /* requeueDelay= */ Durations.fromSeconds(1));
    Thread thread = new Thread(dispatchedMonitor);
    thread.start();
    while (!readyForInterrupt.get()) {
      TimeUnit.MICROSECONDS.sleep(1);
    }
    thread.interrupt();
    thread.join();
  }

  @Test
  public void shouldIterateUntilShouldStop() throws IOException {
    Scannable<DispatchedOperation> location = mock(Scannable.class);
    when(location.scan(any(Integer.class), any(String.class), any(Consumer.class)))
        .thenReturn(SENTINEL_PAGE_TOKEN);
    BooleanSupplier shouldStop = mock(BooleanSupplier.class);
    when(shouldStop.getAsBoolean()).thenReturn(false).thenReturn(true);
    DispatchedMonitor dispatchedMonitor =
        new DispatchedMonitor(
            shouldStop, location, requeuer, /* intervalSeconds= */ 0, Durations.fromSeconds(1));
    dispatchedMonitor.run();
    verify(location, atLeastOnce())
        .scan(any(Integer.class), any(String.class), any(Consumer.class));
    verify(shouldStop, times(2)).getAsBoolean();
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
