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

@Log
class DispatchedMonitor implements Runnable {
  private final BooleanSupplier shouldStop;
  private final Scannable<DispatchedOperation> location;
  private final BiFunction<QueueEntry, Duration, ListenableFuture<Void>> requeuer;
  private final int intervalSeconds;

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

  private void testDispatchedOperation(
      long now,
      DispatchedOperation dispatchedOperation,
      Consumer<ListenableFuture<Void>> onFuture) {
    // requeue all operations that are over their dispatched duration time
    if (now >= dispatchedOperation.getRequeueAt()) {
      onFuture.accept(requeueDispatchedExecution(dispatchedOperation, now));
    }
  }

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

  void iterate() throws InterruptedException {
    getOnlyInterruptibly(submitAll());
  }

  private void runInterruptibly() throws InterruptedException {
    while (!shouldStop.getAsBoolean()) {
      if (Thread.currentThread().isInterrupted()) {
        throw new InterruptedException();
      }
      TimeUnit.SECONDS.sleep(intervalSeconds);
      iterate();
    }
  }

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
