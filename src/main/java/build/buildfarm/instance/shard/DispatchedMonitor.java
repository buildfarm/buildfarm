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

import static com.google.common.util.concurrent.Futures.successfulAsList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.String.format;

import build.buildfarm.backplane.Backplane;
import build.buildfarm.v1test.DispatchedOperation;
import build.buildfarm.v1test.QueueEntry;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

class DispatchedMonitor implements Runnable {
  private static final Logger logger = Logger.getLogger(DispatchedMonitor.class.getName());

  private final Backplane backplane;
  private final Function<QueueEntry, ListenableFuture<Void>> requeuer;
  private final int intervalSeconds;

  DispatchedMonitor(
      Backplane backplane,
      Function<QueueEntry, ListenableFuture<Void>> requeuer,
      int intervalSeconds) {
    this.backplane = backplane;
    this.requeuer = requeuer;
    this.intervalSeconds = intervalSeconds;
  }

  private ListenableFuture<Void> requeueDispatchedOperation(DispatchedOperation o, long now) {
    QueueEntry queueEntry = o.getQueueEntry();
    String operationName = queueEntry.getExecuteEntry().getOperationName();

    logOverdueOperation(o, now);
    ListenableFuture<Void> requeuedFuture = requeuer.apply(queueEntry);
    long startTime = System.nanoTime();
    requeuedFuture.addListener(
        () -> {
          long endTime = System.nanoTime();
          float ms = (endTime - startTime) / 1000000.0f;
          logger.log(
              Level.INFO, format("DispatchedMonitor::run: requeue(%s) %gms", operationName, ms));
        },
        directExecutor());
    return requeuedFuture;
  }

  private void logOverdueOperation(DispatchedOperation o, long now) {

    // log that the dispatched operation is overdue in order to indicate that it should be requeued.
    String operationName = o.getQueueEntry().getExecuteEntry().getOperationName();
    long overdue_amount = now - o.getRequeueAt();
    StringBuilder message = new StringBuilder();
    message.append(
        String.format(
            "DispatchedMonitor: Testing %s because %dms overdue (%d >= %d)",
            operationName, overdue_amount, now, o.getRequeueAt()));
    logger.log(Level.INFO, message.toString());
  }

  private void testDispatchedOperations(
      long now,
      Iterable<DispatchedOperation> dispatchedOperations,
      ImmutableList.Builder<ListenableFuture<Void>> requeuedFutures) {

    // requeue all operations that are over their dispatched duration time
    for (DispatchedOperation o : dispatchedOperations) {
      if (now >= o.getRequeueAt()) {
        requeuedFutures.add(requeueDispatchedOperation(o, now));
      }
    }
  }

  private ListenableFuture<List<Void>> submitAll() {
    ImmutableList.Builder<ListenableFuture<Void>> requeuedFutures = ImmutableList.builder();
    try {
      long now = System.currentTimeMillis(); /* FIXME sync */
      boolean canQueueNow = backplane.canQueue();
      if (canQueueNow) {
        testDispatchedOperations(now, backplane.getDispatchedOperations(), requeuedFutures);
      }
    } catch (Exception e) {
      if (!backplane.isStopped()) {
        logger.log(Level.SEVERE, "error during dispatch evaluation", e);
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
      logger.log(Level.SEVERE, "unexpected exception", cause);
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
    while (!backplane.isStopped()) {
      if (Thread.currentThread().isInterrupted()) {
        throw new InterruptedException();
      }
      TimeUnit.SECONDS.sleep(intervalSeconds);
      iterate();
    }
  }

  @Override
  public synchronized void run() {
    logger.log(Level.INFO, "DispatchedMonitor: Running");
    try {
      runInterruptibly();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      logger.log(Level.INFO, "DispatchedMonitor: Exiting");
    }
  }
}
