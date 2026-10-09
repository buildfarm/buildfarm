// Copyright 2018 The Buildfarm Authors. All rights reserved.
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

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.Command;
import build.buildfarm.common.Claim;
import build.buildfarm.v1test.ExecuteEntry;
import build.buildfarm.v1test.QueueEntry;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ExecuteActionStageTest {
  @Test(timeout = 15000)
  public void waitingForCpuSharesDoesNotBlockExecutionStage() throws Exception {
    Market market = new Market();
    WorkerContext context = mock(WorkerContext.class);
    when(context.market()).thenReturn(market);
    when(context.commandExecutionClaims(any(Command.class))).thenReturn(1);

    ExecuteActionStage stage =
        new ExecuteActionStage(context, mock(PipelineStage.class), mock(PipelineStage.class));
    ExecutionContext firstAction =
        ExecutionContext.newBuilder()
            .setCommand(Command.getDefaultInstance())
            .setClaim(mock(Claim.class))
            .build();
    ExecutionContext secondAction =
        ExecutionContext.newBuilder()
            .setCommand(Command.getDefaultInstance())
            .setClaim(mock(Claim.class))
            .build();

    market.start();
    market.sell(ExecuteActionStage.SHARES_PER_SLOT);
    Thread waitingClaim = new Thread(() -> claim(stage, secondAction), "waiting-cpu-claim");
    CountDownLatch executionStageEntered = new CountDownLatch(1);
    Thread executionStage =
        new Thread(
            () -> {
              synchronized (stage) {
                executionStageEntered.countDown();
              }
            },
            "execution-stage-start");
    try {
      assertThat(stage.claim(firstAction)).isTrue();
      waitingClaim.start();
      assertThat(waitingInCpuLease(waitingClaim)).isTrue();

      // ExecuteActionStage.iterate takes the first action, then enters this monitor to start it.
      // If the second claim holds that monitor while waiting, the first action can never run and
      // return its CPU shares.
      executionStage.start();
      assertThat(executionStageEntered.await(2, TimeUnit.SECONDS)).isTrue();
    } finally {
      market.sell(ExecuteActionStage.SHARES_PER_SLOT);
      waitingClaim.join(5000);
      executionStage.join(5000);
      market.stop();
    }
  }

  private static void claim(ExecuteActionStage stage, ExecutionContext executionContext) {
    try {
      stage.claim(executionContext);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static boolean waitingInCpuLease(Thread thread) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (System.nanoTime() < deadline) {
      if (thread.getState() == Thread.State.WAITING
          && Arrays.stream(thread.getStackTrace())
              .anyMatch(frame -> frame.getClassName().equals(CPULease.class.getName()))) {
        return true;
      }
      Thread.sleep(10);
    }
    return false;
  }

  @Test
  public void errorPathDestroysExecDir() throws Exception {
    WorkerContext context = mock(WorkerContext.class);
    when(context.getExecuteStageWidth()).thenReturn(1);
    PipelineStage error = mock(PipelineStage.class);

    QueueEntry errorEntry =
        QueueEntry.newBuilder()
            .setExecuteEntry(ExecuteEntry.newBuilder().setOperationName("error"))
            .build();
    ExecutionContext errorContext =
        ExecutionContext.newBuilder()
            .setQueueEntry(errorEntry)
            .setExecDir(Path.of("error-operation-path"))
            .build();

    PipelineStage executeActionStage = new ExecuteActionStage(context, /* output= */ null, error);
    executeActionStage.error().put(errorContext);
    verify(context, times(1)).destroyExecDir(errorContext.execDir);
    verify(error, times(1)).put(errorContext);
  }
}
