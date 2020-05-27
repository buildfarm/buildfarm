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
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;

public class PutOperationStage extends PipelineStage.NullStage {
  private final InterruptingConsumer<Operation> onPut;

  private volatile AverageTimeCostOfLastPeriod[] averagesWithinDifferentPeriods;

  public PutOperationStage(InterruptingConsumer<Operation> onPut) {
    this.onPut = onPut;
    this.averagesWithinDifferentPeriods =
        new AverageTimeCostOfLastPeriod[] {
          new AverageTimeCostOfLastPeriod(100),
          new AverageTimeCostOfLastPeriod(60 * 10),
          new AverageTimeCostOfLastPeriod(60 * 60),
          new AverageTimeCostOfLastPeriod(3 * 60 * 60),
          new AverageTimeCostOfLastPeriod(24 * 60 * 60)
        };
  }

  @Override
  public void put(OperationContext operationContext) throws InterruptedException {
    onPut.acceptInterruptibly(operationContext.operation);
    synchronized (this) {
      for (AverageTimeCostOfLastPeriod average : averagesWithinDifferentPeriods) {
        average.addOperation(operationContext);
      }
    }
  }

  public synchronized OperationStageDurations[] getAverageTimeCostPerStage() {
    OperationStageDurations[] results =
        new OperationStageDurations[averagesWithinDifferentPeriods.length];
    for (int i = 0; i < results.length; i++) {
      results[i] = averagesWithinDifferentPeriods[i].getAverageOfLastPeriod();
    }
    return results;
    // return Arrays.stream(averagesWithinDifferentPeriods)
    //    .map(AverageTimeCostOfLastPeriod::getAverageOfLastPeriod)
    //    .toArray(OperationStageDurations[]::new);
  }

  private static class AverageTimeCostOfLastPeriod {
    static final int NumOfSlots = 100;
    private OperationStageDurations[] slots;
    private int lastUsedSlot = -1;
    private int period;
    private OperationStageDurations nextOperation;
    private OperationStageDurations averageTimeCosts;
    private Timestamp lastOperationCompleteTime = null;

    AverageTimeCostOfLastPeriod(int period) {
      this.period = period;
      slots = new OperationStageDurations[NumOfSlots];
      for (int i = 0; i < slots.length; i++) {
        slots[i] = new OperationStageDurations();
      }
      nextOperation = new OperationStageDurations();
      averageTimeCosts = new OperationStageDurations();
    }

    OperationStageDurations getAverageOfLastPeriod() {
      averageTimeCosts.reset();
      for (OperationStageDurations slot : slots) {
        averageTimeCosts.addOperations(slot);
      }
      averageTimeCosts.period = period;
      return averageTimeCosts;
    }

    void addOperation(OperationContext context) {
      ExecutedActionMetadata metadata =
          context.executeResponse.build().getResult().getExecutionMetadata();

      // The duration between the complete time of the new Operation and last Operation
      // added could be longer than the logging period here.
      Timestamp completeTime = metadata.getOutputUploadCompletedTimestamp();
      int currentSlot = (int) completeTime.getSeconds() % period / (period / slots.length);
      if (lastOperationCompleteTime != null && lastUsedSlot >= 0) {
        Duration duration = Timestamps.between(lastOperationCompleteTime, completeTime);
        if (duration.getSeconds() >= (this.period / slots.length)) {
          for (int i = lastUsedSlot + 1; (i % slots.length) <= currentSlot; i++) {
            slots[i % slots.length].reset();
          }
        }
      }
      lastOperationCompleteTime = completeTime;
      lastUsedSlot = currentSlot;

      nextOperation.set(metadata);
      slots[currentSlot].addOperations(nextOperation);
    }
  }

  // when operationCount == 1, an object represents one operation's time costs on each stage;
  // when operationCount > 1, an object represents aggregated time costs of multiple operations.
  public static class OperationStageDurations {
    public float queuedToMatch;
    public float matchToInputFetchStart;
    public float inputFetchStartToComplete;
    public float inputFetchCompleteToExecutionStart;
    public float executionStartToComplete;
    public float executionCompleteToOutputUploadStart;
    public float outputUploadStartToComplete;
    public int operationCount;
    public int period;

    OperationStageDurations() {
      reset();
    }

    void set(ExecutedActionMetadata metadata) {
      queuedToMatch =
          millisecondBetween(metadata.getQueuedTimestamp(), metadata.getWorkerStartTimestamp());
      matchToInputFetchStart =
          millisecondBetween(
              metadata.getWorkerStartTimestamp(), metadata.getInputFetchStartTimestamp());
      inputFetchStartToComplete =
          millisecondBetween(
              metadata.getInputFetchStartTimestamp(), metadata.getInputFetchCompletedTimestamp());
      inputFetchCompleteToExecutionStart =
          millisecondBetween(
              metadata.getInputFetchCompletedTimestamp(), metadata.getExecutionStartTimestamp());
      executionStartToComplete =
          millisecondBetween(
              metadata.getExecutionStartTimestamp(), metadata.getExecutionCompletedTimestamp());
      executionCompleteToOutputUploadStart =
          millisecondBetween(
              metadata.getExecutionCompletedTimestamp(), metadata.getOutputUploadStartTimestamp());
      outputUploadStartToComplete =
          millisecondBetween(
              metadata.getOutputUploadStartTimestamp(),
              metadata.getOutputUploadCompletedTimestamp());
      operationCount = 1;
    }

    void reset() {
      queuedToMatch = 0.0f;
      matchToInputFetchStart = 0.0f;
      inputFetchStartToComplete = 0.0f;
      inputFetchCompleteToExecutionStart = 0.0f;
      executionStartToComplete = 0.0f;
      executionCompleteToOutputUploadStart = 0.0f;
      outputUploadStartToComplete = 0.0f;
      operationCount = 0;
    }

    void addOperations(OperationStageDurations other) {
      this.queuedToMatch =
          computeAverage(
              this.queuedToMatch, this.operationCount, other.queuedToMatch, other.operationCount);
      this.matchToInputFetchStart =
          computeAverage(
              this.matchToInputFetchStart,
              this.operationCount,
              other.matchToInputFetchStart,
              other.operationCount);
      this.inputFetchStartToComplete =
          computeAverage(
              this.inputFetchStartToComplete,
              this.operationCount,
              other.inputFetchStartToComplete,
              other.operationCount);
      this.inputFetchCompleteToExecutionStart =
          computeAverage(
              this.inputFetchCompleteToExecutionStart,
              this.operationCount,
              other.inputFetchCompleteToExecutionStart,
              other.operationCount);
      this.executionStartToComplete =
          computeAverage(
              this.executionStartToComplete,
              this.operationCount,
              other.executionStartToComplete,
              other.operationCount);
      this.executionCompleteToOutputUploadStart =
          computeAverage(
              this.executionCompleteToOutputUploadStart,
              this.operationCount,
              other.executionCompleteToOutputUploadStart,
              other.operationCount);
      this.outputUploadStartToComplete =
          computeAverage(
              this.outputUploadStartToComplete,
              this.operationCount,
              other.outputUploadStartToComplete,
              other.operationCount);
      this.operationCount += other.operationCount;
    }

    private static float computeAverage(
        float time1, int operationCount1, float time2, int operationCount2) {
      if (operationCount1 == 0 && operationCount2 == 0) {
        return 0.0f;
      }
      return (time1 * operationCount1 + time2 * operationCount2)
          / (operationCount1 + operationCount2);
    }

    private static float millisecondBetween(Timestamp from, Timestamp to) {
      // The time unit we want is millisecond.
      // 1 second = 1000 milliseconds
      // 1 millisecond = 1000,000 nanoseconds
      Duration d = Timestamps.between(from, to);
      double interval = d.getSeconds() * 1000.0 + d.getNanos() / (1000.0 * 1000.0);
      return (float) interval;
    }
  }
}
