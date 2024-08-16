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

import com.google.common.collect.Sets;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;
import lombok.extern.java.Log;

@Log
public class InputFetchStage extends SuperscalarPipelineStage {
  private static final Gauge inputFetchSlotUsage =
      Gauge.build().name("input_fetch_slot_usage").help("Input fetch slot Usage.").register();
  private static final Histogram inputFetchTime =
      Histogram.build().name("input_fetch_time_ms").help("Input fetch time in ms.").register();
  private static final Histogram inputFetchStallTime =
      Histogram.build()
          .name("input_fetch_stall_time_ms")
          .help("Input fetch stall time in ms.")
          .register();

  private final Set<Thread> fetchers = Sets.newHashSet();
  private final BlockingQueue<ExecutionContext> queue = new ArrayBlockingQueue<>(1);

  public InputFetchStage(WorkerContext workerContext, PipelineStage output, PipelineStage error) {
    super("InputFetchStage", workerContext, output, error, workerContext.getInputFetchStageWidth());
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
    queue.put(executionContext);
  }

  synchronized int removeAndRelease(String operationName) {
    if (!fetchers.remove(Thread.currentThread())) {
      throw new IllegalStateException("tried to remove unknown fetcher thread");
    }
    releaseClaim(operationName, 1);
    int slotUsage = fetchers.size();
    inputFetchSlotUsage.set(slotUsage);
    return slotUsage;
  }

  public void releaseInputFetcher(
      String operationName, long usecs, long stallUSecs, boolean success) {
    int size = removeAndRelease(operationName);
    inputFetchTime.observe(usecs / 1000.0);
    inputFetchStallTime.observe(stallUSecs / 1000.0);
    complete(
        operationName,
        usecs,
        stallUSecs,
        String.format("%s, %s", success ? "Success" : "Failure", getUsage(size)));
  }

  @Override
  public int getSlotUsage() {
    return fetchers.size();
  }

  @Override
  protected synchronized void interruptAll() {
    for (Thread fetcher : fetchers) {
      fetcher.interrupt();
    }
  }

  @Override
  protected int claimsRequired(ExecutionContext executionContext) {
    return 1;
  }

  @Override
  protected void iterate() throws InterruptedException {
    ExecutionContext executionContext = take();
    Thread fetcher =
        new Thread(
            new InputFetcher(workerContext, executionContext, this), "InputFetchStage.fetcher");

    synchronized (this) {
      fetchers.add(fetcher);
      int slotUsage = fetchers.size();
      inputFetchSlotUsage.set(slotUsage);
      start(executionContext.queueEntry.getExecuteEntry().getOperationName(), getUsage(slotUsage));
      fetcher.start();
    }
  }
}
