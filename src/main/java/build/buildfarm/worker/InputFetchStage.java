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

import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import java.util.logging.Logger;

public class InputFetchStage extends SuperscalarPipelineStage {
  private static final Logger logger = Logger.getLogger(InputFetchStage.class.getName());
  private static final Gauge inputFetchSlotUsage =
      Gauge.build().name("input_fetch_slot_usage").help("Input fetch slot Usage.").register();
  private static final Histogram inputFetchTime =
      Histogram.build().name("input_fetch_time_ms").help("Input fetch time in ms.").register();
  private static final Histogram inputFetchStallTime =
      Histogram.build()
          .name("input_fetch_stall_time_ms")
          .help("Input fetch stall time in ms.")
          .register();

  public InputFetchStage(WorkerContext workerContext, PipelineStage output, PipelineStage error) {
    super("InputFetchStage", workerContext, output, error, workerContext.getInputFetchStageWidth());
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
    slots.intake.put(operationContext);
  }

  synchronized int removeAndRelease(String operationName) {
    if (!slots.jobs.remove(Thread.currentThread())) {
      throw new IllegalStateException("tried to remove unknown fetcher thread");
    }
    releaseClaim(operationName, 1);
    return slots.jobs.size();
  }

  public void releaseInputFetcher(
      String operationName, long usecs, long stallUSecs, boolean success) {
    int size = removeAndRelease(operationName);
    inputFetchTime.observe(usecs / 1000.0);
    inputFetchStallTime.observe(stallUSecs / 1000.0);
    inputFetchSlotUsage.set(size);
    logComplete(
        operationName,
        usecs,
        stallUSecs,
        String.format("%s, %s", success ? "Success" : "Failure", getUsage(size)));
  }

  @Override
  protected synchronized void interruptAll() {
    for (Thread fetcher : slots.jobs) {
      fetcher.interrupt();
    }
  }

  @Override
  protected int claimsRequired(OperationContext operationContext) {
    return 1;
  }

  @Override
  protected void iterate() throws InterruptedException {
    OperationContext operationContext = take();
    Thread fetcher = new Thread(new InputFetcher(workerContext, operationContext, this));

    synchronized (this) {
      slots.jobs.add(fetcher);
      logStart(
          operationContext.queueEntry.getExecuteEntry().getOperationName(),
          getUsage(slots.jobs.size()));
      fetcher.start();
    }
  }
}
