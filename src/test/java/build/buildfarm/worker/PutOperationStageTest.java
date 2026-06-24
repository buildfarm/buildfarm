// Copyright 2024 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.worker;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.ExecutedActionMetadata;
import build.buildfarm.worker.PutOperationStage.OperationStageDurations;
import com.google.longrunning.Operation;
import com.google.protobuf.Timestamp;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PutOperationStageTest {
  // The fixed bucketing periods PutOperationStage tracks, in seconds, in declaration order.
  private static final long[] PERIOD_SECONDS = {60, 10 * 60, 60 * 60, 3 * 60 * 60, 24 * 60 * 60};

  private static Operation operation(boolean done) {
    return Operation.newBuilder().setName("test-operation").setDone(done).build();
  }

  @Test
  public void putForwardsOperationToConsumer() throws InterruptedException {
    List<Operation> received = new ArrayList<>();
    PutOperationStage stage = new PutOperationStage(received::add);

    Operation operation = operation(/* done= */ false);
    stage.put(ExecutionContext.newBuilder().setOperation(operation).build());

    assertThat(received).containsExactly(operation);
  }

  @Test
  public void putPropagatesConsumerInterrupt() {
    PutOperationStage stage =
        new PutOperationStage(
            op -> {
              throw new InterruptedException();
            });

    assertThrows(
        InterruptedException.class,
        () -> stage.put(ExecutionContext.newBuilder().setOperation(operation(false)).build()));
  }

  @Test
  public void doneOperationWithResultRecordsExecutionMetadata() throws InterruptedException {
    List<Operation> received = new ArrayList<>();
    PutOperationStage stage = new PutOperationStage(received::add);

    // Build a done operation whose ExecuteResponse carries a result with execution metadata; this
    // drives the hasResult() branch of put().
    ExecutionContext context =
        ExecutionContext.newBuilder().setOperation(operation(/* done= */ true)).build();
    context.executeResponse.setResult(
        ActionResult.newBuilder().setExecutionMetadata(stageMetadata()).build());

    stage.put(context);

    assertThat(received).hasSize(1);
    // After recording, the per-stage averages remain queryable with all five periods present.
    assertPeriodsPresent(stage.getAverageTimeCostPerStage());
  }

  @Test
  public void doneOperationWithoutResultRecordsPartialMetadata() throws InterruptedException {
    List<Operation> received = new ArrayList<>();
    PutOperationStage stage = new PutOperationStage(received::add);

    // A done operation with no result falls back to the partial execution metadata carried on the
    // context's QueuedOperationMetadata builder.
    ExecutionContext context =
        ExecutionContext.newBuilder().setOperation(operation(/* done= */ true)).build();
    context
        .metadata
        .getExecuteOperationMetadataBuilder()
        .setPartialExecutionMetadata(stageMetadata());

    stage.put(context);

    assertThat(received).hasSize(1);
    assertPeriodsPresent(stage.getAverageTimeCostPerStage());
  }

  @Test
  public void averageTimeCostPerStageExposesConfiguredPeriods() {
    PutOperationStage stage = new PutOperationStage(op -> {});

    assertPeriodsPresent(stage.getAverageTimeCostPerStage());
  }

  // A self-consistent metadata timeline so betweenIfAfter() produces non-negative durations across
  // every stage segment. Absolute values are not asserted: getAverageOfLastPeriod() consults the
  // wall clock and discards stale buckets, so averaged durations are not deterministic.
  private static ExecutedActionMetadata stageMetadata() {
    return ExecutedActionMetadata.newBuilder()
        .setQueuedTimestamp(ts(0))
        .setWorkerStartTimestamp(ts(1))
        .setInputFetchStartTimestamp(ts(2))
        .setInputFetchCompletedTimestamp(ts(3))
        .setExecutionStartTimestamp(ts(4))
        .setExecutionCompletedTimestamp(ts(5))
        .setOutputUploadStartTimestamp(ts(6))
        .setOutputUploadCompletedTimestamp(ts(7))
        .build();
  }

  private static Timestamp ts(long seconds) {
    return Timestamp.newBuilder().setSeconds(seconds).build();
  }

  private static void assertPeriodsPresent(OperationStageDurations[] durations) {
    assertThat(durations).hasLength(PERIOD_SECONDS.length);
    for (int i = 0; i < durations.length; i++) {
      assertThat(durations[i]).isNotNull();
      assertThat(durations[i].period.getSeconds()).isEqualTo(PERIOD_SECONDS[i]);
    }
  }
}
