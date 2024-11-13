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

import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import java.util.logging.Logger;
import javax.annotation.concurrent.GuardedBy;
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

  @GuardedBy("this")
  private int slotUsage;

  public InputFetchStage(WorkerContext workerContext, PipelineStage output, PipelineStage error) {
    super(
        "InputFetchStage",
        "fetcher",
        workerContext,
        output,
        error,
        workerContext.getInputFetchStageWidth());
  }

  @Override
  protected Logger getLogger() {
    return log;
  }

  synchronized int removeAndRelease(String operationName) {
    releaseClaim(operationName, 1);
    slotUsage--;
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
  protected int claimsRequired(ExecutionContext executionContext) {
    return 1;
  }

  @Override
  protected void iterate() throws InterruptedException {
    ExecutionContext executionContext = take();
    InputFetcher inputFetcher =
        new InputFetcher(workerContext, executionContext, this, pollerExecutor);

    synchronized (this) {
      slotUsage++;
      inputFetchSlotUsage.set(slotUsage);
      start(executionContext.queueEntry.getExecuteEntry().getOperationName(), getUsage(slotUsage));
      executor.execute(inputFetcher);
    }
  }
}
