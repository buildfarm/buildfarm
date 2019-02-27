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

import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

public class ExecuteActionStage extends PipelineStage {
  private static final Logger logger = Logger.getLogger(ExecuteActionStage.class.getName());

  private final Set<Thread> executors = new HashSet<>();
  private BlockingQueue<OperationContext> queue = new ArrayBlockingQueue<>(1);

  public ExecuteActionStage(WorkerContext workerContext, PipelineStage output, PipelineStage error) {
    super("ExecuteActionStage", workerContext, output, createDestroyExecDirStage(workerContext, error));
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
  public void put(OperationContext operationContext) throws InterruptedException {
    queue.put(operationContext);
  }

  @Override
  public OperationContext take() throws InterruptedException {
    return queue.take();
  }

  @Override
  public synchronized boolean claim() throws InterruptedException {
    if (isClosed()) {
      return false;
    }

    while (executors.size() + queue.size() >= workerContext.getExecuteStageWidth()) {
      wait();
    }
    return true;
  }

  public synchronized int removeAndNotify() {
    if (!executors.remove(Thread.currentThread())) {
      throw new IllegalStateException("tried to remove unknown executor thread");
    }
    this.notify();
    return executors.size();
  }

  private String getUsage(int size) {
    return String.format("%s/%d", size, workerContext.getExecuteStageWidth());
  }

  private void logComplete(int size) {
    logger.info(String.format("%s: %s", name, getUsage(size)));
  }

  @Override
  public void release() {
    // do nothing. no state is maintained for claims
  }

  public void releaseExecutor(String operationName, long usecs, long stallUSecs, int exitCode) {
    int size = removeAndNotify();
    logComplete(
        operationName,
        usecs,
        stallUSecs,
        String.format("exit code: %d, %s", exitCode, getUsage(size)));
  }

  @Override
  protected synchronized boolean isClaimed() {
    return Iterables.any(executors, (executor) -> executor.isAlive());
  }

  @Override
  protected void iterate() throws InterruptedException {
    OperationContext operationContext = take();
    Thread executor = new Thread(new Executor(workerContext, operationContext, this));

    int size;
    synchronized (this) {
      executors.add(executor);
      size = executors.size();
    }
    logStart(operationContext.operation.getName(), getUsage(size));

    executor.start();
  }
}
