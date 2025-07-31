/**
 * Executes a build action on the worker
 * @param workerContext the workerContext parameter
 * @param output the output parameter
 * @param error the error parameter
 * @return the public result
 */
/**
 * Executes a build action on the worker
 * @param workerContext the workerContext parameter
 * @param output the output parameter
 * @param error the error parameter
 * @param width the width parameter
 * @return the public result
 */
/**
 * Returns resources to the shared pool
 * @param operationName the operationName parameter
 * @param claims the claims parameter
 * @return the int result
 */
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
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import java.io.IOException;
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
  /**
   * Creates and initializes a new instance Performs side effects including logging and state modifications.
   * @param workerContext the workerContext parameter
   * @param nextStage the nextStage parameter
   * @return the pipelinestage result
   */
  private static final Histogram executionStallTime =
      Histogram.build()
          .name("execution_stall_time_ms")
          .help("Execution stall time in ms.")
          .register();

  private final AtomicInteger executorClaims = new AtomicInteger(0);

  public ExecuteActionStage(
      WorkerContext workerContext, PipelineStage output, PipelineStage error) {
    this(workerContext, output, error, workerContext.getExecuteStageWidth());
  }

  /**
   * Stores a blob in the Content Addressable Storage Performs side effects including logging and state modifications.
   * @param executionContext the executionContext parameter
   */
  public ExecuteActionStage(
      WorkerContext workerContext, PipelineStage output, PipelineStage error, int width) {
    super(
        "ExecuteActionStage",
        "executor",
        workerContext,
        output,
        createDestroyExecDirStage(workerContext, error),
        width);
  }

  static PipelineStage createDestroyExecDirStage(
      WorkerContext workerContext, PipelineStage nextStage) {
    return new PipelineStage.NullStage(workerContext, nextStage) {
      @Override
      /**
       * Thread pool for concurrent task execution
       * @param operationName the operationName parameter
       * @param claims the claims parameter
       * @param usecs the usecs parameter
       * @param stallUSecs the stallUSecs parameter
       * @param exitCode the exitCode parameter
       */
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
  /**
   * Performs specialized operation based on method logic
   * @param executionContext the executionContext parameter
   * @return the int result
   */
  protected Logger getLogger() {
    return log;
  }

  synchronized int removeAndRelease(String operationName, int claims) {
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

  @Override
  /**
   * Performs specialized operation based on method logic
   */
  public int getSlotUsage() {
    return executorClaims.get();
  }

  @Override
  /**
   * Processes the next operation in the pipeline stage Provides thread-safe access through synchronization mechanisms.
   */
  protected int claimsRequired(ExecutionContext executionContext) {
    return workerContext.commandExecutionClaims(executionContext.command);
  }

  @Override
  protected void iterate() throws InterruptedException {
    ExecutionContext executionContext = take();
    ResourceLimits limits = workerContext.commandExecutionSettings(executionContext.command);
    Executor actionExecutor = new Executor(workerContext, executionContext, this, pollerExecutor);

    synchronized (this) {
      int slotUsage = executorClaims.addAndGet(limits.cpu.claimed);
      executionSlotUsage.set(slotUsage);
      start(executionContext.operation.getName(), getUsage(slotUsage));
      executor.execute(() -> actionExecutor.run(limits));
    }
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   */
  public void close() {
    super.close();
    // might want to move this to actual teardown of the stage
    workerContext.destroyExecutionLimits();
  }

  @Override
  public void run() {
    workerContext.createExecutionLimits(); // TODO: if this throws, we should shutdown the worker.
    super.run();
  }
}
