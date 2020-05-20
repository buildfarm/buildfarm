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
import com.google.protobuf.Timestamp;

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
    Timestamp[] timestamps =
        new Timestamp[] {
          metadata.getQueuedTimestamp(),
          metadata.getWorkerStartTimestamp(),
          metadata.getInputFetchStartTimestamp(),
          metadata.getInputFetchCompletedTimestamp(),
          metadata.getExecutionStartTimestamp(),
          metadata.getExecutionCompletedTimestamp(),
          metadata.getOutputUploadStartTimestamp(),
          metadata.getOutputUploadCompletedTimestamp(),
        };

    float[] times = new float[timestamps.length];
    // The time unit we want is millisecond.
    // 1 second = 1000 milliseconds
    // 1 millisecond = 1000,000 nanoseconds
    for (int i = 0; i< times.length; i++) {
      times[i] = timestamps[i].getSeconds() * 1000.0f + timestamps[i].getNanos() / (1000.0f * 1000.0f);
    }

    // [
    //  queued                -> worker_start(MatchStage),
    //  worker_start          -> input_fetch_start,
    //  input_fetch_start     -> input_fetch_completed,
    //  input_fetch_completed -> execution_start,
    //  execution_start       -> execution_completed,
    //  execution_completed   -> output_upload_start,
    //  output_upload_start   -> output_upload_completed
    // ]
    float[] results = new float[times.length - 1];
    for (int i = 0; i < results.length; i++) {
      results[i] = (times[i + 1] - times[i]);
    }

    for (int i = 0; i < operationAverageTimes.length; i++) {
      operationAverageTimes[i] =
          (operationCount * operationAverageTimes[i] + results[i]) / (operationCount + 1);
    }
  }
}
