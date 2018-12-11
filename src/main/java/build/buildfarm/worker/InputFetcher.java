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
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.WARNING;

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

  private long runInterruptibly(Stopwatch stopwatch) throws InterruptedException {
    final String operationName = operationContext.operation.getName();
    final Thread fetcherThread = Thread.currentThread();
    Poller poller = workerContext.createPoller(
        "InputFetcher",
        operationContext.queueEntry,
        QUEUED,
        () -> fetcherThread.interrupt(),
        Deadline.after(60, SECONDS));

    workerContext.logInfo("InputFetcher: Fetching inputs: " + operationName);

    long fetchStartAt = stopwatch.elapsed(MICROSECONDS);

    Path execDir;
    try {
      execDir = workerContext.createExecDir(
          operationName,
          operationContext.directoriesIndex,
          operationContext.action,
          operationContext.command);
    } catch (IOException e) {
      logger.log(WARNING, "error creating exec dir for " + operationName, e);

      owner.error().put(operationContext);
      return 0;
    } finally {
      poller.stop();
    }
    success = true;

    long fetchUSecs = stopwatch.elapsed(MICROSECONDS);
    Duration fetchedIn = Durations.fromMicros(fetchUSecs - fetchStartAt);

    OperationContext executeOperationContext = operationContext.toBuilder()
        .setExecDir(execDir)
        .setFetchedIn(fetchedIn)
        .build();
    if (owner.output().claim()) {
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
    String operationName = operationContext.operation.getName();
    Stopwatch stopwatch = Stopwatch.createStarted();
    try {
      stallUSecs = runInterruptibly(stopwatch);
    } catch (InterruptedException e) {
      /* we can be interrupted when the poller fails */
      try {
        owner.error().put(operationContext);
      } catch (InterruptedException errorEx) {
        logger.log(SEVERE, "interrupted while erroring " + operationName, errorEx);
      }
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      logger.log(SEVERE, "error while fetching inputs for " + operationName, e);
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
