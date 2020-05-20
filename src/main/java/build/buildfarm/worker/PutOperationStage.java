// Copyright 2018 The Bazel Authors. All rights reserved.
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

import build.bazel.remote.execution.v2.ExecutedActionMetadata;
import build.buildfarm.common.function.InterruptingConsumer;
import com.google.longrunning.Operation;
import java.util.logging.Level;

public class PutOperationStage extends PipelineStage.NullStage {
  private final InterruptingConsumer<Operation> onPut;

  private int operationCount = 0;
  private boolean startToCount = false;
  private float[] operationAverageTimes = new float[7];

  public PutOperationStage(InterruptingConsumer<Operation> onPut) {
    this.onPut = onPut;
  }

  @Override
  public void put(OperationContext operationContext) throws InterruptedException {
    onPut.acceptInterruptibly(operationContext.operation);
    synchronized (this) {
      if (startToCount) {
        computeOperationTime(operationContext);
        operationCount++;
      }
    }
  }

  public synchronized int getOperationCount() {
    startToCount = true;
    int currentCount = operationCount;
    operationCount = 0;
    return currentCount;
  }

  public synchronized float[] getAverageOperationTimes() {
    float[] currentOperationAverageTimes = operationAverageTimes;
    operationAverageTimes = new float[7];
    return currentOperationAverageTimes;
  }

  private void computeOperationTime(OperationContext context) {
    ExecutedActionMetadata metadata =
        context.executeResponse.build().getResult().getExecutionMetadata();
    getLogger().log(Level.WARNING, String.format("NAME OF THE OPERATION: %s", context.operation.getName()));
    float[] timestamps =
        new float[] {
          metadata.getQueuedTimestamp().getNanos(),
          metadata.getWorkerStartTimestamp().getNanos(),
          metadata.getInputFetchStartTimestamp().getNanos(),
          metadata.getInputFetchCompletedTimestamp().getNanos(),
          metadata.getExecutionStartTimestamp().getNanos(),
          metadata.getExecutionCompletedTimestamp().getNanos(),
          metadata.getOutputUploadStartTimestamp().getNanos(),
          metadata.getOutputUploadCompletedTimestamp().getNanos(),
        };
    getLogger().log(Level.WARNING, String.format("NAME OF THE OPERATION: %s", context.operation.getName()));

    // [
    //  queued                -> worker_start(MatchStage),
    //  worker_start          -> input_fetch_start,
    //  input_fetch_start     -> input_fetch_completed,
    //  input_fetch_completed -> execution_start,
    //  execution_start       -> execution_completed,
    //  execution_completed   -> output_upload_start,
    //  output_upload_start   -> output_upload_completed
    // ]
    float nanoToMilli = (float) Math.pow(10.0, 6.0);
    float[] results = new float[timestamps.length - 1];
    for (int i = 0; i < results.length; i++) {
      results[i] = (timestamps[i + 1] - timestamps[i]) / nanoToMilli;
    }

    for (int i = 0; i < operationAverageTimes.length; i++) {
      operationAverageTimes[i] =
          (operationCount * operationAverageTimes[i] + results[i]) / (operationCount + 1);
    }
  }
}
