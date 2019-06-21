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

import static java.util.logging.Level.SEVERE;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

public class ExecuteActionStage extends SuperscalarPipelineStage {
  private static final Logger logger = Logger.getLogger(ExecuteActionStage.class.getName());

  private final Set<Thread> executors = Sets.newHashSet();
  private BlockingQueue<OperationContext> queue = new ArrayBlockingQueue<>(1);

  public ExecuteActionStage(WorkerContext workerContext, PipelineStage output, PipelineStage error) {
    super(
        "ExecuteActionStage",
        workerContext,
        output,
        createDestroyExecDirStage(workerContext, error),
        workerContext.getExecuteStageWidth());
  }

  static PipelineStage createDestroyExecDirStage(WorkerContext workerContext, PipelineStage nextStage) {
    return new PipelineStage.NullStage(workerContext, nextStage) {
      @Override
      public void put(OperationContext operationContext) throws InterruptedException {
        try {
          workerContext.destroyExecDir(operationContext.execDir);
        } catch (IOException e) {
          logger.log(SEVERE, "error while destroying action root " + operationContext.execDir, e);
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
    return takeOrDrain(queue);
  }

  @Override
  public void put(OperationContext operationContext) throws InterruptedException {
    queue.put(operationContext);
  }

  synchronized int removeAndRelease(String operationName) {
    if (!executors.remove(Thread.currentThread())) {
      throw new IllegalStateException("tried to remove unknown executor thread for " + operationName);
    }
    releaseClaim(operationName);
    return executors.size();
  }

  public void releaseExecutor(String operationName, long usecs, long stallUSecs, int exitCode) {
    int size = removeAndRelease(operationName);
    logComplete(
        operationName,
        usecs,
        stallUSecs,
        String.format("exit code: %d, %s", exitCode, getUsage(size)));
  }

  @Override
  protected synchronized void interruptAll() {
    for (Thread executor : executors) {
      executor.interrupt();
    }
  }

  @Override
  protected void iterate() throws InterruptedException {
    OperationContext operationContext = take();
    Thread executor = new Thread(new Executor(workerContext, operationContext, this));

    synchronized (this) {
      executors.add(executor);
      logStart(operationContext.operation.getName(), getUsage(executors.size()));
      executor.start();
    }
  }
}
