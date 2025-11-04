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

import static com.google.common.util.concurrent.Futures.successfulAsList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.String.format;

import build.buildfarm.common.Scannable;
import build.buildfarm.v1test.DispatchedOperation;
import build.buildfarm.v1test.QueueEntry;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.logging.Level;
import lombok.extern.java.Log;

/**
 * Monitors dispatched operations and requeues operations that have exceeded their dispatch timeout.
 *
 * <p>This monitor runs continuously in a background thread, periodically scanning all dispatched
 * operations and requeuing any that have been dispatched longer than their configured timeout
 * duration. This ensures that operations don't get stuck indefinitely on unresponsive workers.
 *
 * <p>The monitor uses a configurable interval to check operations and compares the current time
 * against each operation's {@code requeueAt} timestamp to determine if requeuing is needed.
 */
@Log
class DispatchedMonitor implements Runnable {
  private final BooleanSupplier shouldStop;
  private final Scannable<DispatchedOperation> location;
  private final BiFunction<QueueEntry, Duration, ListenableFuture<Void>> requeuer;
  private final int intervalSeconds;

  /**
   * Constructs a new DispatchedMonitor.
   *
   * @param shouldStop supplier that returns {@code true} when the monitor should stop running
   * @param location scannable source of dispatched operations to monitor
   * @param requeuer function that requeues a queue entry with a specified delay duration
   * @param intervalSeconds number of seconds to wait between monitoring iterations
   */
  DispatchedMonitor(
      BooleanSupplier shouldStop,
      Scannable<DispatchedOperation> location,
      BiFunction<QueueEntry, Duration, ListenableFuture<Void>> requeuer,
      int intervalSeconds) {
    this.shouldStop = shouldStop;
    this.location = location;
    this.requeuer = requeuer;
    this.intervalSeconds = intervalSeconds;
  }

  /**
   * Requeues a dispatched operation that has exceeded its dispatch timeout.
   *
   * <p>This method logs the overdue operation, initiates the requeue process with a 60-second
   * delay, and adds a listener to track and log the requeue operation's completion time.
   *
   * @param o the dispatched operation to requeue
   * @param now the current timestamp in milliseconds used for calculating overdue duration
   * @return a listenable future that completes when the requeue operation finishes
   */
  private ListenableFuture<Void> requeueDispatchedExecution(DispatchedOperation o, long now) {
    QueueEntry queueEntry = o.getQueueEntry();
    String operationName = queueEntry.getExecuteEntry().getOperationName();

    logOverdueOperation(o, now);
    ListenableFuture<Void> requeuedFuture = requeuer.apply(queueEntry, Durations.fromSeconds(60));
    long startTime = System.nanoTime();
    requeuedFuture.addListener(
        () -> {
          long endTime = System.nanoTime();
          float ms = (endTime - startTime) / 1000000.0f;
          log.log(
              Level.INFO, format("DispatchedMonitor::run: requeue(%s) %gms", operationName, ms));
        },
        directExecutor());
    return requeuedFuture;
  }

  /**
   * Logs information about an overdue dispatched operation.
   *
   * <p>Creates a diagnostic log message indicating how long the operation has been overdue and the
   * timestamp comparison that triggered the requeue decision.
   *
   * @param o the overdue dispatched operation
   * @param now the current timestamp in milliseconds
   */
  private void logOverdueOperation(DispatchedOperation o, long now) {
    // log that the dispatched operation is overdue in order to indicate that it should be requeued.
    String operationName = o.getQueueEntry().getExecuteEntry().getOperationName();
    long overdue_amount = now - o.getRequeueAt();
    String message =
        String.format(
            "DispatchedMonitor: Testing %s because %dms overdue (%d >= %d)",
            operationName, overdue_amount, now, o.getRequeueAt());
    log.log(Level.INFO, message);
  }

  /**
   * Tests whether a dispatched operation should be requeued based on its timeout.
   *
   * <p>If the current time has reached or exceeded the operation's requeue timestamp, this method
   * initiates a requeue and passes the resulting future to the provided consumer.
   *
   * @param now the current timestamp in milliseconds
   * @param dispatchedOperation the dispatched operation to test
   * @param onFuture consumer that receives the requeue future if the operation is overdue
   */
  private void testDispatchedOperation(
      long now,
      DispatchedOperation dispatchedOperation,
      Consumer<ListenableFuture<Void>> onFuture) {
    // requeue all operations that are over their dispatched duration time
    if (now >= dispatchedOperation.getRequeueAt()) {
      onFuture.accept(requeueDispatchedExecution(dispatchedOperation, now));
    }
  }

  /**
   * Scans all dispatched operations and submits overdue operations for requeuing.
   *
   * <p>This method iterates through all dispatched operations in pages of 100, testing each one to
   * determine if it should be requeued. All requeue futures are collected and returned as a single
   * future that completes successfully even if individual requeue operations fail.
   *
   * <p>The scan continues until all operations have been processed or the monitor is signaled to
   * stop. Any exceptions during scanning are logged unless the monitor is stopping.
   *
   * @return a future that completes when all requeue operations have finished, containing a list of
   *     results (successful operations return null, failed operations return null due to
   *     successfulAsList behavior)
   */
  private ListenableFuture<List<Void>> submitAll() {
    ImmutableList.Builder<ListenableFuture<Void>> requeuedFutures = ImmutableList.builder();
    try {
      long now = System.currentTimeMillis(); /* FIXME sync */
      String token = Scannable.SENTINEL_PAGE_TOKEN;
      do {
        token =
            location.scan(
                100,
                token,
                dispatchedOperation ->
                    testDispatchedOperation(now, dispatchedOperation, requeuedFutures::add));
      } while (!token.equals(Scannable.SENTINEL_PAGE_TOKEN) && !shouldStop.getAsBoolean());
    } catch (Exception e) {
      if (!shouldStop.getAsBoolean()) {
        log.log(Level.SEVERE, "error during dispatch evaluation", e);
      }
    }
    return successfulAsList(requeuedFutures.build());
  }

  /**
   * Waits for a future to complete, allowing interruption but handling execution exceptions.
   *
   * <p>This method blocks until the future completes. If an execution exception occurs (which is
   * unlikely when using successfulAsList), it extracts and rethrows the cause as a runtime
   * exception.
   *
   * @param <T> the type of the future's result
   * @param future the future to wait for
   * @return the result of the future once it completes
   * @throws InterruptedException if the thread is interrupted while waiting
   * @throws RuntimeException if the future completes with a runtime exception
   * @throws UncheckedExecutionException if the future completes with a checked exception
   */
  static <T> T getOnlyInterruptibly(ListenableFuture<T> future) throws InterruptedException {
    try {
      return future.get();
    } catch (ExecutionException e) {
      // unlikely, successfulAsList prevents this as the only return
      Throwable cause = e.getCause();
      log.log(Level.SEVERE, "unexpected exception", cause);
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      throw new UncheckedExecutionException(cause);
    }
  }

  /**
   * Performs a single iteration of the monitoring cycle.
   *
   * <p>This method scans all dispatched operations and waits for all requeue operations to complete
   * before returning.
   *
   * @throws InterruptedException if the thread is interrupted while waiting for requeue operations
   */
  void iterate() throws InterruptedException {
    getOnlyInterruptibly(submitAll());
  }

  /**
   * Runs the monitor loop until stopped or interrupted.
   *
   * <p>This method repeatedly sleeps for the configured interval and then performs a monitoring
   * iteration. It continues until the shouldStop supplier returns true or the thread is
   * interrupted.
   *
   * @throws InterruptedException if the thread is interrupted during sleep or iteration
   */
  private void runInterruptibly() throws InterruptedException {
    while (!shouldStop.getAsBoolean()) {
      if (Thread.currentThread().isInterrupted()) {
        throw new InterruptedException();
      }
      TimeUnit.SECONDS.sleep(intervalSeconds);
      iterate();
    }
  }

  /**
   * Executes the monitoring loop when the monitor is started as a thread.
   *
   * <p>This method is synchronized to ensure only one monitoring thread can run at a time. It logs
   * the start and exit of the monitor, and properly handles interruption by restoring the interrupt
   * status when exiting.
   *
   * <p>The monitor will continue running until interrupted or signaled to stop via the shouldStop
   * supplier.
   */
  @Override
  public synchronized void run() {
    log.log(Level.INFO, "DispatchedMonitor: Running");
    try {
      runInterruptibly();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      log.log(Level.INFO, "DispatchedMonitor: Exiting");
    }
  }
}
