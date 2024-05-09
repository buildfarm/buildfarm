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
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import java.util.Arrays;

public class PutOperationStage extends PipelineStage.NullStage {
  private final InterruptingConsumer<Operation> onPut;

  private final AverageTimeCostOfLastPeriod[] averagesWithinDifferentPeriods;

  public PutOperationStage(InterruptingConsumer<Operation> onPut) {
    this.onPut = onPut;
    this.averagesWithinDifferentPeriods =
        new AverageTimeCostOfLastPeriod[] {
          new AverageTimeCostOfLastPeriod(60), // 1 minute
          new AverageTimeCostOfLastPeriod(10 * 60), // 10 minutes
          new AverageTimeCostOfLastPeriod(60 * 60), // 1 hour
          new AverageTimeCostOfLastPeriod(3 * 60 * 60), // 3 hours
          new AverageTimeCostOfLastPeriod(24 * 60 * 60) // 24 hours
        };
  }

  @Override
  public void put(OperationContext operationContext) throws InterruptedException {
    onPut.acceptInterruptibly(operationContext.operation);
    synchronized (this) {
      if (operationContext.operation.getDone()) {
        for (AverageTimeCostOfLastPeriod average : averagesWithinDifferentPeriods) {
          average.addOperation(
              operationContext.executeResponse.build().getResult().getExecutionMetadata());
        }
      }
    }
  }

  public synchronized OperationStageDurations[] getAverageTimeCostPerStage() {
    return Arrays.stream(averagesWithinDifferentPeriods)
        .map(AverageTimeCostOfLastPeriod::getAverageOfLastPeriod)
        .toArray(OperationStageDurations[]::new);
  }

  private static class AverageTimeCostOfLastPeriod {
    static final int NumOfSlots = 100;
    private final OperationStageDurations[] buckets;
    private int lastUsedSlot = -1;
    private final Duration period;
    private final OperationStageDurations nextOperation;
    private OperationStageDurations averageTimeCosts;
    private Timestamp lastOperationCompleteTime;

    AverageTimeCostOfLastPeriod(int period) {
      this.period = Durations.fromSeconds(period);
      buckets = new OperationStageDurations[NumOfSlots];
      for (int i = 0; i < buckets.length; i++) {
        buckets[i] = new OperationStageDurations();
      }
      nextOperation = new OperationStageDurations();
      averageTimeCosts = new OperationStageDurations();
    }

    private void removeStaleData(Timestamp now) {
      // currentSlot != lastUsedSlot means stepping over to a new bucket.
      // The data in the new bucket should be thrown away before storing new data.
      int currentSlot = getCurrentSlot(now);
      if (lastOperationCompleteTime != null && lastUsedSlot >= 0) {
        Duration duration = Timestamps.between(lastOperationCompleteTime, now);
        // if 1) duration between the new added operation and last added one is longer than period
        // or 2) the duration is shorter than period but longer than time range of a single bucket
        //       and at the same time currentSlot == lastUsedSlot
        if ((Durations.toMillis(duration) >= Durations.toMillis(period))
            || (lastUsedSlot == currentSlot
                && Durations.toMillis(duration) > (Durations.toMillis(period) / buckets.length))) {
          for (OperationStageDurations bucket : buckets) {
            bucket.reset();
          }
        } else if (lastUsedSlot != currentSlot) {
          // currentSlot < lastUsedSlot means wrap around happened
          currentSlot = currentSlot < lastUsedSlot ? currentSlot + NumOfSlots : currentSlot;
          for (int i = lastUsedSlot + 1; i <= currentSlot; i++) {
            buckets[i % buckets.length].reset();
          }
        }
      }
    }

    // Compute the bucket index the Timestamp would belong to
    private int getCurrentSlot(Timestamp time) {
      long millisPeriod = Durations.toMillis(period);
      return (int) (Timestamps.toMillis(time) % millisPeriod / (millisPeriod / buckets.length));
    }

    OperationStageDurations getAverageOfLastPeriod() {
      // creating a Timestamp representing now to trigger stale data throwing away
      Timestamp now = Timestamps.fromMillis(System.currentTimeMillis());
      removeStaleData(now);

      // compute unweighted average of all buckets
      averageTimeCosts.reset();
      for (OperationStageDurations bucket : buckets) {
        averageTimeCosts.addOperations(bucket.computeAverage(bucket.operationCount));
      }
      averageTimeCosts = averageTimeCosts.computeAverage(buckets.length);
      averageTimeCosts.period = period;
      return averageTimeCosts;
    }

    void addOperation(ExecutedActionMetadata metadata) {
      // remove stale data first
      Timestamp completeTime = metadata.getOutputUploadCompletedTimestamp();
      removeStaleData(completeTime);

      // add new ExecutedOperation metadata
      int currentSlot = getCurrentSlot(completeTime);
      nextOperation.set(metadata);
      buckets[currentSlot].addOperations(nextOperation);

      lastOperationCompleteTime = completeTime;
      lastUsedSlot = currentSlot;
    }
  }

  private static Duration betweenIfAfter(Timestamp a, Timestamp b) {
    if (Timestamps.compare(a, b) > 0) {
      return Timestamps.between(a, b);
    }
    return Duration.getDefaultInstance();
  }

  // when operationCount == 1, an object represents one operation's time costs on each stage;
  // when operationCount > 1, an object represents aggregated time costs of multiple operations.
  public static class OperationStageDurations {
    public Duration queuedToMatch;
    public Duration matchToInputFetchStart;
    public Duration inputFetchStartToComplete;
    public Duration inputFetchCompleteToExecutionStart;
    public Duration executionStartToComplete;
    public Duration executionCompleteToOutputUploadStart;
    public Duration outputUploadStartToComplete;
    public int operationCount;
    public Duration period;

    OperationStageDurations() {
      reset();
    }

    void reset() {
      queuedToMatch = Duration.getDefaultInstance();
      matchToInputFetchStart = Duration.getDefaultInstance();
      inputFetchStartToComplete = Duration.getDefaultInstance();
      inputFetchCompleteToExecutionStart = Duration.getDefaultInstance();
      executionStartToComplete = Duration.getDefaultInstance();
      executionCompleteToOutputUploadStart = Duration.getDefaultInstance();
      outputUploadStartToComplete = Duration.getDefaultInstance();
      operationCount = 0;
    }

    void set(ExecutedActionMetadata metadata) {
      queuedToMatch =
          betweenIfAfter(metadata.getQueuedTimestamp(), metadata.getWorkerStartTimestamp());
      matchToInputFetchStart =
          betweenIfAfter(
              metadata.getWorkerStartTimestamp(), metadata.getInputFetchStartTimestamp());
      inputFetchStartToComplete =
          betweenIfAfter(
              metadata.getInputFetchStartTimestamp(), metadata.getInputFetchCompletedTimestamp());
      inputFetchCompleteToExecutionStart =
          betweenIfAfter(
              metadata.getInputFetchCompletedTimestamp(), metadata.getExecutionStartTimestamp());
      executionStartToComplete =
          betweenIfAfter(
              metadata.getExecutionStartTimestamp(), metadata.getExecutionCompletedTimestamp());
      executionCompleteToOutputUploadStart =
          betweenIfAfter(
              metadata.getExecutionCompletedTimestamp(), metadata.getOutputUploadStartTimestamp());
      outputUploadStartToComplete =
          betweenIfAfter(
              metadata.getOutputUploadStartTimestamp(),
              metadata.getOutputUploadCompletedTimestamp());
      operationCount = 1;
    }

    void addOperations(OperationStageDurations other) {
      this.queuedToMatch = Durations.add(this.queuedToMatch, other.queuedToMatch);
      this.matchToInputFetchStart =
          Durations.add(this.matchToInputFetchStart, other.matchToInputFetchStart);
      this.inputFetchStartToComplete =
          Durations.add(this.inputFetchStartToComplete, other.inputFetchStartToComplete);
      this.inputFetchCompleteToExecutionStart =
          Durations.add(
              this.inputFetchCompleteToExecutionStart, other.inputFetchCompleteToExecutionStart);
      this.executionStartToComplete =
          Durations.add(this.executionStartToComplete, other.executionStartToComplete);
      this.executionCompleteToOutputUploadStart =
          Durations.add(
              this.executionCompleteToOutputUploadStart,
              other.executionCompleteToOutputUploadStart);
      this.outputUploadStartToComplete =
          Durations.add(this.outputUploadStartToComplete, other.outputUploadStartToComplete);
      this.operationCount += other.operationCount;
    }

    private OperationStageDurations computeAverage(int weight) {
      OperationStageDurations average = new OperationStageDurations();
      if (weight == 0) {
        return average;
      }
      average.queuedToMatch = Durations.fromNanos(Durations.toNanos(this.queuedToMatch) / weight);
      average.matchToInputFetchStart =
          Durations.fromNanos(Durations.toNanos(this.matchToInputFetchStart) / weight);
      average.inputFetchStartToComplete =
          Durations.fromNanos(Durations.toNanos(this.inputFetchStartToComplete) / weight);
      average.inputFetchCompleteToExecutionStart =
          Durations.fromNanos(Durations.toNanos(this.inputFetchCompleteToExecutionStart) / weight);
      average.executionStartToComplete =
          Durations.fromNanos(Durations.toNanos(this.executionStartToComplete) / weight);
      average.executionCompleteToOutputUploadStart =
          Durations.fromNanos(
              Durations.toNanos(this.executionCompleteToOutputUploadStart) / weight);
      average.outputUploadStartToComplete =
          Durations.fromNanos(Durations.toNanos(this.outputUploadStartToComplete) / weight);
      average.operationCount = this.operationCount;

      return average;
    }
  }
}
