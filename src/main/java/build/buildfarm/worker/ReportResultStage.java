/**
 * Publishes execution results to the distributed system
 * @param workerContext the workerContext parameter
 * @param output the output parameter
 * @param error the error parameter
 * @return the public result
 */
/**
 * Returns resources to the shared pool
 * @param operationName the operationName parameter
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

import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import java.util.logging.Logger;
import javax.annotation.concurrent.GuardedBy;
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

  @GuardedBy("this")
  private int slotUsage;

  /**
   * Returns resources to the shared pool
   * @param operationName the operationName parameter
   * @param usecs the usecs parameter
   * @param stallUSecs the stallUSecs parameter
   * @param success the success parameter
   */
  public ReportResultStage(WorkerContext workerContext, PipelineStage output, PipelineStage error) {
    super(
        "ReportResultStage",
        "reporter",
        workerContext,
        output,
        error,
        workerContext.getReportResultStageWidth());
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

  synchronized int removeAndRelease(String operationName) {
    releaseClaim(operationName, 1);
    slotUsage--;
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
  /**
   * Processes the next operation in the pipeline stage Provides thread-safe access through synchronization mechanisms.
   */
  protected int claimsRequired(ExecutionContext executionContext) {
    return 1;
  }

  @Override
  protected void iterate() throws InterruptedException {
    ExecutionContext executionContext = take();
    ResultReporter reporter =
        new ResultReporter(workerContext, executionContext, this, pollerExecutor);

    synchronized (this) {
      slotUsage++;
      reportResultSlotUsage.set(slotUsage);
      start(executionContext.queueEntry.getExecuteEntry().getOperationName(), getUsage(slotUsage));
      executor.execute(reporter);
    }
  }
}
