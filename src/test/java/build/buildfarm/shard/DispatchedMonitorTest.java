// Copyright 2026 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.shard;

import static build.buildfarm.common.Scannable.SENTINEL_PAGE_TOKEN;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.truth.Truth.assertThat;
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
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;
import io.prometheus.client.CollectorRegistry;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import redis.clients.jedis.exceptions.JedisDataException;

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

  private DispatchedMonitor earlyRequeueMonitor(
      QueueEntry queueEntry, DispatchedMonitor.Lease lease) {
    Iterable<DispatchedOperation> dispatchedOperations =
        ImmutableList.of(
            DispatchedOperation.newBuilder().setRequeueAt(0).setQueueEntry(queueEntry).build());
    when(requeuer.apply(eq(queueEntry), any(Duration.class))).thenReturn(immediateFuture(null));
    return new DispatchedMonitor(
        /* shouldStop= */ () -> false,
        new IterableScannable(dispatchedOperations),
        requeuer,
        /* intervalSeconds= */ 0,
        /* requeueDelay= */ Durations.fromSeconds(1),
        lease,
        /* server= */ "test-server");
  }

  private static QueueEntry queueEntryNamed(String operationName) {
    return QueueEntry.newBuilder()
        .setExecuteEntry(ExecuteEntry.newBuilder().setOperationName(operationName).build())
        .build();
  }

  @Test
  public void shouldNotScanWithoutLease() throws Exception {
    Scannable<DispatchedOperation> location = mock(Scannable.class);
    DispatchedMonitor dispatchedMonitor =
        new DispatchedMonitor(
            /* shouldStop= */ () -> false,
            location,
            requeuer,
            /* intervalSeconds= */ 0,
            /* requeueDelay= */ Durations.fromSeconds(1),
            /* lease= */ () -> false,
            /* server= */ "test-server");
    dispatchedMonitor.iterate();
    verifyNoInteractions(location);
    verifyNoInteractions(requeuer);
  }

  @Test
  public void shouldRequeueWithLease() throws Exception {
    QueueEntry queueEntry = queueEntryNamed("operation-with-lease");
    earlyRequeueMonitor(queueEntry, /* lease= */ () -> true).iterate();
    verify(requeuer, times(1)).apply(queueEntry, Durations.fromSeconds(1));
  }

  @Test
  public void shouldRequeueWhenLeaseAcquisitionFails() throws Exception {
    QueueEntry queueEntry = queueEntryNamed("operation-with-failed-lease");
    earlyRequeueMonitor(
            queueEntry,
            /* lease= */ () -> {
              throw new IOException("backplane unavailable");
            })
        .iterate();
    verify(requeuer, times(1)).apply(queueEntry, Durations.fromSeconds(1));
  }

  @Test
  public void shouldAcquireLeaseEachIteration() throws Exception {
    AtomicInteger acquisitions = new AtomicInteger();
    QueueEntry queueEntry = queueEntryNamed("operation-with-alternating-lease");
    DispatchedMonitor dispatchedMonitor =
        earlyRequeueMonitor(queueEntry, /* lease= */ () -> acquisitions.incrementAndGet() % 2 == 1);
    dispatchedMonitor.iterate();
    dispatchedMonitor.iterate();
    dispatchedMonitor.iterate();
    assertThat(acquisitions.get()).isEqualTo(3);
    verify(requeuer, times(2)).apply(queueEntry, Durations.fromSeconds(1));
  }

  /** A lease that counts renewals and lasts {@code durationMillis}. */
  private static class CountingLease implements DispatchedMonitor.Lease {
    final AtomicInteger renewals = new AtomicInteger();
    private final long durationMillis;
    private final boolean stillHeld;

    CountingLease(long durationMillis, boolean stillHeld) {
      this.durationMillis = durationMillis;
      this.stillHeld = stillHeld;
    }

    @Override
    public boolean tryAcquire() {
      return true;
    }

    @Override
    public boolean renew() {
      renewals.incrementAndGet();
      return stillHeld;
    }

    @Override
    public long durationMillis() {
      return durationMillis;
    }
  }

  /** Runs one iteration whose single requeue completes after {@code requeueMillis}. */
  private void iterateWithSlowRequeue(DispatchedMonitor.Lease lease, long requeueMillis)
      throws Exception {
    QueueEntry queueEntry = queueEntryNamed("operation-with-slow-requeue");
    SettableFuture<Void> requeued = SettableFuture.create();
    when(requeuer.apply(eq(queueEntry), any(Duration.class))).thenReturn(requeued);
    DispatchedMonitor dispatchedMonitor =
        new DispatchedMonitor(
            /* shouldStop= */ () -> false,
            new IterableScannable(
                ImmutableList.of(
                    DispatchedOperation.newBuilder()
                        .setRequeueAt(0)
                        .setQueueEntry(queueEntry)
                        .build())),
            requeuer,
            /* intervalSeconds= */ 0,
            /* requeueDelay= */ Durations.fromSeconds(1),
            lease,
            /* server= */ "test-server");
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    try {
      executor.schedule(() -> requeued.set(null), requeueMillis, TimeUnit.MILLISECONDS);
      dispatchedMonitor.iterate();
    } finally {
      executor.shutdownNow();
    }
    verify(requeuer, times(1)).apply(queueEntry, Durations.fromSeconds(1));
  }

  @Test
  public void shouldRenewLeaseWhileIterationRuns() throws Exception {
    CountingLease lease = new CountingLease(/* durationMillis= */ 100, /* stillHeld= */ true);
    iterateWithSlowRequeue(lease, /* requeueMillis= */ 500);
    // renewed about every 50ms while waiting, plus once when the iteration finishes
    assertThat(lease.renewals.get()).isAtLeast(4);
  }

  @Test
  public void shouldRenewLeaseOnceAfterShortIteration() throws Exception {
    CountingLease lease = new CountingLease(/* durationMillis= */ 60000, /* stillHeld= */ true);
    earlyRequeueMonitor(queueEntryNamed("operation-with-short-iteration"), lease).iterate();
    assertThat(lease.renewals.get()).isEqualTo(1);
  }

  @Test
  public void shouldStopRenewingLostLeaseButFinishIteration() throws Exception {
    CountingLease lease = new CountingLease(/* durationMillis= */ 100, /* stillHeld= */ false);
    iterateWithSlowRequeue(lease, /* requeueMillis= */ 500);
    assertThat(lease.renewals.get()).isEqualTo(1);
  }

  @Test
  public void shouldNotRenewLeaseWhenAcquisitionFails() throws Exception {
    AtomicInteger renewals = new AtomicInteger();
    DispatchedMonitor.Lease lease =
        new DispatchedMonitor.Lease() {
          @Override
          public boolean tryAcquire() throws IOException {
            throw new IOException("backplane unavailable");
          }

          @Override
          public boolean renew() {
            renewals.incrementAndGet();
            return true;
          }

          @Override
          public long durationMillis() {
            return 100;
          }
        };
    iterateWithSlowRequeue(lease, /* requeueMillis= */ 300);
    assertThat(renewals.get()).isEqualTo(0);
  }

  private static double iterations(String server, String outcome) {
    Double value =
        CollectorRegistry.defaultRegistry.getSampleValue(
            "dispatched_monitor_iterations_total",
            new String[] {"server", "outcome"},
            new String[] {server, outcome});
    return value == null ? 0 : value;
  }

  private DispatchedMonitor monitorOnServer(String server, DispatchedMonitor.Lease lease) {
    return new DispatchedMonitor(
        /* shouldStop= */ () -> false,
        new IterableScannable(ImmutableList.of()),
        requeuer,
        /* intervalSeconds= */ 0,
        /* requeueDelay= */ Durations.fromSeconds(1),
        lease,
        server);
  }

  @Test
  public void shouldCountScannedIterationsForServer() throws Exception {
    DispatchedMonitor dispatchedMonitor = monitorOnServer("pod-scanned", () -> true);
    dispatchedMonitor.iterate();
    dispatchedMonitor.iterate();
    assertThat(iterations("pod-scanned", DispatchedMonitor.OUTCOME_SCANNED)).isEqualTo(2.0);
    assertThat(iterations("pod-scanned", DispatchedMonitor.OUTCOME_SKIPPED)).isEqualTo(0.0);
  }

  @Test
  public void shouldCountSkippedIterationsForServer() throws Exception {
    monitorOnServer("pod-skipped", () -> false).iterate();
    assertThat(iterations("pod-skipped", DispatchedMonitor.OUTCOME_SKIPPED)).isEqualTo(1.0);
    assertThat(iterations("pod-skipped", DispatchedMonitor.OUTCOME_SCANNED)).isEqualTo(0.0);
  }

  @Test
  public void shouldCountIterationsScannedWithoutLeaseForServer() throws Exception {
    monitorOnServer(
            "pod-lease-error",
            () -> {
              throw new IOException("backplane unavailable");
            })
        .iterate();
    assertThat(iterations("pod-lease-error", DispatchedMonitor.OUTCOME_SCANNED_WITHOUT_LEASE))
        .isEqualTo(1.0);
  }

  @Test
  public void shouldRenewLeaseWhilePageProcessingBlocks() throws Exception {
    Scannable<DispatchedOperation> location = mock(Scannable.class);
    // one page whose processing blocks for longer than the lease, as when each overdue operation
    // synchronously fetches its execution
    when(location.scan(any(Integer.class), any(String.class), any(Consumer.class)))
        .thenAnswer(
            invocation -> {
              Thread.sleep(500);
              return SENTINEL_PAGE_TOKEN;
            });
    CountingLease lease = new CountingLease(/* durationMillis= */ 100, /* stillHeld= */ true);
    new DispatchedMonitor(
            /* shouldStop= */ () -> false,
            location,
            requeuer,
            /* intervalSeconds= */ 0,
            /* requeueDelay= */ Durations.fromSeconds(1),
            lease,
            /* server= */ "test-server")
        .iterate();
    // renewed about every 50ms while the page blocks, plus once when the iteration finishes
    assertThat(lease.renewals.get()).isAtLeast(4);
  }

  @Test
  public void shouldScanWhenLeaseAcquisitionIsRejected() throws Exception {
    QueueEntry queueEntry = queueEntryNamed("operation-with-rejected-lease");
    earlyRequeueMonitor(
            queueEntry,
            /* lease= */ () -> {
              throw new JedisDataException("NOPERM this user has no permissions to run 'set'");
            })
        .iterate();
    verify(requeuer, times(1)).apply(queueEntry, Durations.fromSeconds(1));
  }

  @Test
  public void shouldKeepIteratingWhenLeaseRenewalIsRejected() throws Exception {
    AtomicInteger acquisitions = new AtomicInteger();
    DispatchedMonitor.Lease lease =
        new DispatchedMonitor.Lease() {
          @Override
          public boolean tryAcquire() {
            acquisitions.incrementAndGet();
            return true;
          }

          @Override
          public boolean renew() {
            throw new JedisDataException("NOPERM this user has no permissions to run 'eval'");
          }

          @Override
          public long durationMillis() {
            return 60000;
          }
        };
    QueueEntry queueEntry = queueEntryNamed("operation-with-rejected-renewal");
    BooleanSupplier shouldStop = mock(BooleanSupplier.class);
    when(shouldStop.getAsBoolean()).thenReturn(false, false, true);
    when(requeuer.apply(eq(queueEntry), any(Duration.class))).thenReturn(immediateFuture(null));
    new DispatchedMonitor(
            shouldStop,
            new IterableScannable(
                ImmutableList.of(
                    DispatchedOperation.newBuilder()
                        .setRequeueAt(0)
                        .setQueueEntry(queueEntry)
                        .build())),
            requeuer,
            /* intervalSeconds= */ 0,
            /* requeueDelay= */ Durations.fromSeconds(1),
            lease,
            /* server= */ "test-server")
        .run();
    // the final renewal of each iteration throws, and the monitor still runs a second iteration
    assertThat(acquisitions.get()).isEqualTo(2);
    verify(requeuer, times(2)).apply(queueEntry, Durations.fromSeconds(1));
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
