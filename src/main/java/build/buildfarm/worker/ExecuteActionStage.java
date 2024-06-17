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
import com.google.common.collect.Sets;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import lombok.extern.java.Log;

@Log
public class ExecuteActionStage extends SuperscalarPipelineStage {
  private static final Gauge executionSlotUsage =
      Gauge.build().name("execution_slot_usage").help("Execution slot Usage.").register();
  private static final Histogram executionTime =
      Histogram.build().name("execution_time_ms").help("Execution time in ms.").register();
  private static final Histogram executionStallTime =
      Histogram.build()
          .name("execution_stall_time_ms")
          .help("Execution stall time in ms.")
          .register();

  private final Set<Thread> executors = Sets.newHashSet();
  private final AtomicInteger executorClaims = new AtomicInteger(0);
  private final BlockingQueue<ExecutionContext> queue = new ArrayBlockingQueue<>(1);

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
      public void put(ExecutionContext executionContext) throws InterruptedException {
        try {
          workerContext.destroyExecDir(executionContext.execDir);
        } catch (IOException e) {
          log.log(
              Level.SEVERE, "error while destroying action root " + executionContext.execDir, e);
        } finally {
          output.put(executionContext);
        }
      }
    };
  }

  @Override
  protected Logger getLogger() {
    return log;
  }

  @Override
  public ExecutionContext take() throws InterruptedException {
    return takeOrDrain(queue);
  }

  @Override
  public void put(ExecutionContext executionContext) throws InterruptedException {
    while (!isClosed() && !output.isClosed()) {
      if (queue.offer(executionContext, 10, TimeUnit.MILLISECONDS)) {
        return;
      }
    }
    throw new InterruptedException("stage closed");
  }

  synchronized int removeAndRelease(String operationName, int claims) {
    if (!executors.remove(Thread.currentThread())) {
      throw new IllegalStateException(
          "tried to remove unknown executor thread for " + operationName);
    }
    releaseClaim(operationName, claims);
    int slotUsage = executorClaims.addAndGet(-claims);
    executionSlotUsage.set(slotUsage);
    return slotUsage;
  }

  public void releaseExecutor(
      String operationName, int claims, long usecs, long stallUSecs, int exitCode) {
    int slotUsage = removeAndRelease(operationName, claims);
    executionTime.observe(usecs / 1000.0);
    executionStallTime.observe(stallUSecs / 1000.0);
    complete(
        operationName,
        usecs,
        stallUSecs,
        String.format("exit code: %d, %s", exitCode, getUsage(slotUsage)));
  }

  public int getSlotUsage() {
    return executorClaims.get();
  }

  @Override
  protected synchronized void interruptAll() {
    for (Thread executor : executors) {
      executor.interrupt();
    }
  }

  @Override
  protected int claimsRequired(ExecutionContext executionContext) {
    return workerContext.commandExecutionClaims(executionContext.command);
  }

  @Override
  protected void iterate() throws InterruptedException {
    ExecutionContext executionContext = take();
    ResourceLimits limits = workerContext.commandExecutionSettings(executionContext.command);
    Executor executor = new Executor(workerContext, executionContext, this);
    Thread executorThread = new Thread(() -> executor.run(limits), "ExecuteActionStage.executor");

    synchronized (this) {
      executors.add(executorThread);
      int slotUsage = executorClaims.addAndGet(limits.cpu.claimed);
      executionSlotUsage.set(slotUsage);
      start(executionContext.operation.getName(), getUsage(slotUsage));
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
