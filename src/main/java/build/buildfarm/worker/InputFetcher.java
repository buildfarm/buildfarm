// Copyright 2017 The Bazel Authors. All rights reserved.
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

package build.buildfarm.worker;

import static build.bazel.remote.execution.v2.ExecuteOperationMetadata.Stage.QUEUED;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.WARNING;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.Command;
import build.buildfarm.common.Poller;
import build.buildfarm.v1test.QueuedOperation;
import com.google.common.base.Stopwatch;
import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;
import io.grpc.Deadline;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

public class InputFetcher implements Runnable {
  private static final Logger logger = Logger.getLogger(InputFetcher.class.getName());

  private final WorkerContext workerContext;
  private final OperationContext operationContext;
  private final InputFetchStage owner;
  private boolean success = false;

  InputFetcher(WorkerContext workerContext, OperationContext operationContext, InputFetchStage owner) {
    this.workerContext = workerContext;
    this.operationContext = operationContext;
    this.owner = owner;
  }

  private boolean isQueuedOperationValid(QueuedOperation queuedOperation) {
    Action action = queuedOperation.getAction();

    if (action.hasTimeout() && workerContext.hasMaximumActionTimeout()) {
      Duration timeout = action.getTimeout();
      Duration maximum = workerContext.getMaximumActionTimeout();
      if (timeout.getSeconds() > maximum.getSeconds() ||
          (timeout.getSeconds() == maximum.getSeconds() && timeout.getNanos() > maximum.getNanos())) {
        return false;
      }
    }

    return !queuedOperation.getCommand().getArgumentsList().isEmpty();
  }

  private long runInterruptibly(Stopwatch stopwatch) throws InterruptedException {
    final Thread fetcherThread = Thread.currentThread();
    workerContext.resumePoller(
        operationContext.poller,
        "InputFetcher",
        operationContext.queueEntry,
        QUEUED,
        () -> fetcherThread.interrupt(),
        Deadline.after(60, SECONDS));
    try {
      return fetchPolled(stopwatch);
    } finally {
      operationContext.poller.pause();
    }
  }

  private long fetchPolled(Stopwatch stopwatch) throws InterruptedException {
    String operationName = operationContext.queueEntry
        .getExecuteEntry().getOperationName();
    logger.info(format("fetching inputs: %s", operationName));

    long fetchStartAt = stopwatch.elapsed(MICROSECONDS);

    QueuedOperation queuedOperation;
    Path execDir;
    try {
      queuedOperation = workerContext.getQueuedOperation(operationContext.queueEntry);
      if (queuedOperation == null
          || !isQueuedOperationValid(queuedOperation)) {
        logger.severe(format("invalid queued operation: %s", operationName));
        owner.error().put(operationContext);
        return 0;
      }

      execDir = workerContext.createExecDir(
          operationName,
          queuedOperation.getDirectoriesList(),
          queuedOperation.getAction(),
          queuedOperation.getCommand());
    } catch (IOException e) {
      logger.log(WARNING, "error creating exec dir for " + operationName, e);
      owner.error().put(operationContext);
      return 0;
    }
    success = true;

    long fetchUSecs = stopwatch.elapsed(MICROSECONDS);
    Duration fetchedIn = Durations.fromMicros(fetchUSecs - fetchStartAt);

    OperationContext executeOperationContext = operationContext.toBuilder()
        .setExecDir(execDir)
        .setFetchedIn(fetchedIn)
        .setAction(queuedOperation.getAction())
        .setCommand(queuedOperation.getCommand())
        .build();
    boolean claimed = owner.output().claim();
    operationContext.poller.pause();
    if (claimed) {
      try {
        owner.output().put(executeOperationContext);
      } catch (InterruptedException e) {
        owner.output().release();
        throw e;
      }
    } else {
      workerContext.logInfo("InputFetcher: Operation " + operationName + " Failed to claim output");

      owner.error().put(operationContext);
    }
    return stopwatch.elapsed(MICROSECONDS) - fetchUSecs;
  }

  @Override
  public void run() {
    long stallUSecs = 0;
    String operationName = operationContext.queueEntry
        .getExecuteEntry().getOperationName();
    Stopwatch stopwatch = Stopwatch.createStarted();
    try {
      stallUSecs = runInterruptibly(stopwatch);
    } catch (InterruptedException e) {
      /* we can be interrupted when the poller fails */
      try {
        owner.error().put(operationContext);
      } catch (InterruptedException errorEx) {
        logger.log(SEVERE, "interrupted while erroring " + operationName, errorEx);
      } finally {
        Thread.currentThread().interrupt();
      }
    } catch (Exception e) {
      logger.log(WARNING, "error while fetching inputs: " + operationName, e);
      try {
        owner.error().put(operationContext);
      } catch (InterruptedException errorEx) {
        logger.log(SEVERE, "interrupted while erroring " + operationName, errorEx);
      }
      throw e;
    } finally {
      owner.releaseInputFetcher(
          operationName,
          stopwatch.elapsed(MICROSECONDS),
          stallUSecs,
          success);
    }
  }
}
