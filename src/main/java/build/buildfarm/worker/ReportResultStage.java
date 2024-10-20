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

import com.google.common.collect.Sets;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;
import lombok.extern.java.Log;

@Log
public class ReportResultStage extends SuperscalarPipelineStage {
  private static final Gauge reportResultSlotUsage =
      Gauge.build().name("report_result_slot_usage").help("Report result slot Usage.").register();
  private static final Histogram reportResultTime =
      Histogram.build().name("report_result_time_ms").help("Report result time in ms.").register();
  private static final Histogram reportResultStallTime =
      Histogram.build()
          .name("report_result_stall_time_ms")
          .help("Report result stall time in ms.")
          .register();

  private final Set<Thread> reporters = Sets.newHashSet();
  private final BlockingQueue<ExecutionContext> queue = new ArrayBlockingQueue<>(1);

  public ReportResultStage(WorkerContext workerContext, PipelineStage output, PipelineStage error) {
    super(
        "ReportResultStage",
        workerContext,
        output,
        error,
        workerContext.getReportResultStageWidth());
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
    if (!reporters.remove(Thread.currentThread())) {
      throw new IllegalStateException("tried to remove unknown reporter thread");
    }
    releaseClaim(operationName, 1);
    int slotUsage = reporters.size();
    reportResultSlotUsage.set(slotUsage);
    return slotUsage;
  }

  public void releaseResultReporter(
      String operationName, long usecs, long stallUSecs, boolean success) {
    int size = removeAndRelease(operationName);
    reportResultTime.observe(usecs / 1000.0);
    reportResultStallTime.observe(stallUSecs / 1000.0);
    complete(
        operationName,
        usecs,
        stallUSecs,
        String.format("%s, %s", success ? "Success" : "Failure", getUsage(size)));
  }

  @Override
  public int getSlotUsage() {
    return reporters.size();
  }

  @Override
  protected synchronized void interruptAll() {
    for (Thread reporter : reporters) {
      reporter.interrupt();
    }
  }

  @Override
  protected int claimsRequired(ExecutionContext executionContext) {
    return 1;
  }

  @Override
  protected void iterate() throws InterruptedException {
    ExecutionContext executionContext = take();
    Thread reporter =
        new Thread(
            new ResultReporter(workerContext, executionContext, this),
            "ReportResultStage.reporter");

    synchronized (this) {
      reporters.add(reporter);
      int slotUsage = reporters.size();
      reportResultSlotUsage.set(slotUsage);
      start(executionContext.queueEntry.getExecuteEntry().getOperationName(), getUsage(slotUsage));
      reporter.start();
    }
  }
}
