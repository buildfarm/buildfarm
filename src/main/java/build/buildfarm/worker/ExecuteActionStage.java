// Copyright 2017 The Buildfarm Authors. All rights reserved.
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
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import lombok.extern.java.Log;

@Log
public class ExecuteActionStage extends PipelineStage {
  private static final int IDLE_STAGE_CLOSED_MS = 10;
  private static final TimeUnit IDLE_STAGE_CLOSED_UNIT = TimeUnit.MILLISECONDS;

  public static final int SHARES_PER_SLOT = 1000;

  private final BlockingQueue<ExecutionContext> queue = new ArrayBlockingQueue<>(1);
  private final ExecutorService executor = Executors.newCachedThreadPool();
  private final Map<String, Executor> executions = new ConcurrentHashMap<>();

  private static PipelineStage createDestroyExecDirStage(
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

  public ExecuteActionStage(
      WorkerContext workerContext, PipelineStage output, PipelineStage error) {
    super(
        "ExecuteActionStage",
        workerContext,
        output,
        createDestroyExecDirStage(workerContext, error));
  }

  @Override
  Logger getLogger() {
    return log;
  }

  @Override
  ExecutionContext take() throws InterruptedException {
    boolean interrupted = false;
    InterruptedException exception;
    try {
      while (!isClosed() && !output.isClosed()) {
        ExecutionContext context = queue.poll(IDLE_STAGE_CLOSED_MS, IDLE_STAGE_CLOSED_UNIT);
        if (context != null) {
          return context;
        }
      }
      exception = new InterruptedException();
    } catch (InterruptedException e) {
      // only possible way to be terminated
      exception = e;
      // clear interrupted flag
      interrupted = Thread.interrupted();
    }
    waitForRelease(queue);
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
    throw exception;
  }

  @Override
  public int getSlotUsage() {
    return executions.size();
  }

  @Override
  public Iterable<String> getOperationNames() {
    return executions.keySet();
  }

  public Iterable<Executor> getExecutions() {
    return executions.values();
  }

  private void start(String operationName, Executor executor) {
    executions.put(operationName, executor);
    super.start(operationName, "Starting");
  }

  @Override
  protected void complete(String operationName, long usecs, long stallUSecs, String status) {
    super.complete(operationName, usecs, stallUSecs, status);
    executions.remove(operationName);
  }

  @Override
  protected void iterate() throws InterruptedException {
    if (!workerContext.inGracefulShutdown() && isPaused()) {
      return;
    }
    ExecutionContext executionContext = take();
    ResourceLimits limits = workerContext.commandExecutionSettings(executionContext.command);
    Executor actionExecutor =
        new Executor(
            workerContext,
            executionContext,
            /* owner= */ this,
            limits.cpuShareFloor,
            limits.pctMinUnused,
            limits.pctMinThrottled,
            limits.minSharesSold,
            limits.maxSharesSold,
            executor);

    synchronized (this) {
      start(executionContext.operation.getName(), actionExecutor);
      executor.execute(() -> actionExecutor.run(limits));
    }
  }

  @Override
  void put(ExecutionContext executionContext) throws InterruptedException {
    while (!isClosed() && !output.isClosed()) {
      if (queue.offer(executionContext, IDLE_STAGE_CLOSED_MS, IDLE_STAGE_CLOSED_UNIT)) {
        return;
      }
    }
    throw new InterruptedException("stage closed");
  }

  @Override
  public void close() {
    super.close();
    // might want to move this to actual teardown of the stage
    executor.shutdown();
    boolean interrupted = false;
    try {
      if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
        getLogger().severe("executor did not terminate");
      }
    } catch (InterruptedException e) {
      getLogger().severe("executor await shutdown interrupted");
      interrupted = Thread.interrupted() || true;
    }
    workerContext.destroyExecutionLimits();
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void run() {
    workerContext.createExecutionLimits(); // TODO: if this throws, we should shutdown the worker.
    super.run();
  }

  public int getSharesConfigured() {
    return workerContext.getExecuteStageWidth() * SHARES_PER_SLOT;
  }

  public int getSharesUsed() {
    return getSharesConfigured() - getCpuStock();
  }

  private int getCpuStock() {
    return workerContext.market().balance();
  }

  public synchronized boolean claim(ExecutionContext executionContext) throws InterruptedException {
    // issue blocking buy order for SHARES_PER_SLOT * claims
    int slots =
        executionContext.marketExecution
            ? 1
            : workerContext.commandExecutionClaims(executionContext.command);
    int sharesToBuy = SHARES_PER_SLOT * slots;

    CPULease lease = new CPULease(sharesToBuy, workerContext.market());
    executionContext.claim.add(lease);

    return true;
  }

  @Override
  protected boolean isClaimed() {
    return !executions.isEmpty() || workerContext.market().balance() != getSharesConfigured();
  }

  synchronized void waitForRelease(BlockingQueue<ExecutionContext> queue) {
    boolean interrupted = false;
    while (isClaimed()) {
      if (output.isClosed()) {
        // interrupt the currently running threads, because they have nowhere to go
        executor.shutdownNow();
      }
      ExecutionContext executionContext = queue.poll();
      if (executionContext != null) {
        // we need to sell the cpu this process has bought
        executionContext.claim.release();
      } else {
        try {
          wait(/* timeout= */ IDLE_STAGE_CLOSED_MS);
        } catch (InterruptedException e) {
          interrupted = Thread.interrupted() || interrupted;
          // ignore, we will throw it eventually
        }
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  void releaseExecutor(String executionName, long usecs, long stallUSecs, int exitCode) {
    complete(executionName, usecs, stallUSecs, String.format("exit code: %d", exitCode));
  }
}
