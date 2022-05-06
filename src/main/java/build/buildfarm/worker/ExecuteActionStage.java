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

import build.buildfarm.worker.resources.ResourceLimits;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ExecuteActionStage extends SuperscalarPipelineStage {
  private static final Logger logger = Logger.getLogger(ExecuteActionStage.class.getName());
  private static final Gauge executionSlotUsage =
      Gauge.build().name("execution_slot_usage").help("Execution slot Usage.").register();
  private static final Histogram executionTime =
      Histogram.build().name("execution_time_ms").help("Execution time in ms.").register();
  private static final Histogram executionStallTime =
      Histogram.build()
          .name("execution_stall_time_ms")
          .help("Execution stall time in ms.")
          .register();

  public ExecuteActionStage(
      WorkerContext workerContext, PipelineStage output, PipelineStage error) {
    super(
        "ExecuteActionStage",
        workerContext,
        output,
        createDestroyExecDirStage(workerContext, error),
        workerContext.getExecuteStageWidth());
  }

  static PipelineStage createDestroyExecDirStage(
      WorkerContext workerContext, PipelineStage nextStage) {
    return new PipelineStage.NullStage(workerContext, nextStage) {
      @Override
      public void put(OperationContext operationContext) throws InterruptedException {
        try {
          workerContext.destroyExecDir(operationContext.execDir);
        } catch (IOException e) {
          logger.log(
              Level.SEVERE, "error while destroying action root " + operationContext.execDir, e);
        } finally {
          output.put(operationContext);
        }
      }
    };
  }

  @Override
  protected Logger getLogger() {
    return logger;
  }

  @Override
  public OperationContext take() throws InterruptedException {
    return takeOrDrain(slots.intake);
  }

  @Override
  public void put(OperationContext operationContext) throws InterruptedException {
    while (!isClosed() && !output.isClosed()) {
      if (slots.intake.offer(operationContext, 10, TimeUnit.MILLISECONDS)) {
        return;
      }
    }
    throw new InterruptedException("stage closed");
  }

  synchronized void removeAndRelease(String operationName, int claims) {
    if (!slots.jobs.remove(Thread.currentThread())) {
      throw new IllegalStateException(
          "tried to remove unknown executor thread for " + operationName);
    }
    releaseClaim(operationName, claims);
  }

  public void releaseExecutor(
      String operationName, int claims, long usecs, long stallUSecs, int exitCode) {
    removeAndRelease(operationName, claims);
    executionTime.observe(usecs / 1000.0);
    executionStallTime.observe(stallUSecs / 1000.0);

    int size = getSlotUsage();
    executionSlotUsage.set(size);
    logComplete(
        operationName,
        usecs,
        stallUSecs,
        String.format("exit code: %d, %s", exitCode, getUsage(size)));
  }

  @Override
  protected synchronized void interruptAll() {
    for (Thread executor : slots.jobs) {
      executor.interrupt();
    }
  }

  @Override
  protected int claimsRequired(OperationContext operationContext) {
    return workerContext.commandExecutionClaims(operationContext.command);
  }

  @Override
  protected void iterate() throws InterruptedException {
    OperationContext operationContext = take();
    ResourceLimits limits = workerContext.commandExecutionSettings(operationContext.command);
    Executor executor = new Executor(workerContext, operationContext, this);
    Thread executorThread = new Thread(() -> executor.run(limits));

    synchronized (this) {
      slots.jobs.add(executorThread);
      int size = getSlotUsage();
      logStart(operationContext.operation.getName(), getUsage(size));
      executorThread.start();
    }
  }

  @Override
  public void close() {
    super.close();
    workerContext.destroyExecutionLimits();
  }

  @Override
  public void run() {
    workerContext.createExecutionLimits();
    super.run();
  }
}
