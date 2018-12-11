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

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

public class InputFetchStage extends PipelineStage {
  private static final Logger logger = Logger.getLogger(InputFetchStage.class.getName());

  private final Set<Thread> fetchers = Sets.newHashSet();
  private final BlockingQueue<OperationContext> queue = new ArrayBlockingQueue<>(1);

  public InputFetchStage(WorkerContext workerContext, PipelineStage output, PipelineStage error) {
    super("InputFetchStage", workerContext, output, error);
  }

  @Override
  protected Logger getLogger() {
    return logger;
  }

  @Override
  public OperationContext take() throws InterruptedException {
    return queue.take();
  }

  @Override
  public void put(OperationContext operationContext) throws InterruptedException {
    queue.put(operationContext);
  }

  @Override
  public synchronized boolean claim() throws InterruptedException {
    if (isClosed()) {
      return false;
    }

    while (fetchers.size() + queue.size() >= workerContext.getInputFetchStageWidth()) {
      wait();
    }
    return true;
  }

  public synchronized int removeAndNotify() {
    if (!fetchers.remove(Thread.currentThread())) {
      throw new IllegalStateException("tried to remove unknown fetcher thread");
    }
    this.notify();
    return fetchers.size();
  }

  private String getUsage(int size) {
    return String.format("%s/%d", size, workerContext.getInputFetchStageWidth());
  }

  private void logComplete(int size) {
    logger.info(String.format("%s: %s", name, getUsage(size)));
  }

  @Override
  public void release() {
    removeAndNotify();
  }

  public void releaseInputFetcher(String operationName, long usecs, long stallUSecs, boolean success) {
    int size = removeAndNotify();
    logComplete(
        operationName,
        usecs,
        stallUSecs,
        String.format("%s, %s", success ? "Success" : "Failure", getUsage(size)));
  }

  @Override
  protected synchronized boolean isClaimed() {
    return Iterables.any(fetchers, (fetcher) -> fetcher.isAlive());
  }

  @Override
  protected void iterate() throws InterruptedException {
    OperationContext operationContext = take();
    Thread fetcher = new Thread(new InputFetcher(workerContext, operationContext, this));

    int size;
    synchronized (this) {
      fetchers.add(fetcher);
      size = fetchers.size();
    }
    logStart(operationContext.operation.getName(), getUsage(size));

    fetcher.start();
  }
}
