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

import static build.bazel.remote.execution.v2.ExecutionStage.Value.QUEUED;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import build.buildfarm.common.Poller;
import build.buildfarm.v1test.ExecuteEntry;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.QueuedOperation;
import com.google.common.collect.Lists;
import com.google.longrunning.Operation;
import io.grpc.Deadline;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.Predicate;
import java.util.logging.Logger;
import lombok.Getter;
import lombok.extern.java.Log;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Log
public class InputFetchStageTest {
  static class PipelineSink extends PipelineStage {
    @Getter private final List<ExecutionContext> executionContexts = Lists.newArrayList();
    private final Predicate<ExecutionContext> onPutShouldClose;

    PipelineSink(Predicate<ExecutionContext> onPutShouldClose) {
      super("Sink", null, null, null);
      this.onPutShouldClose = onPutShouldClose;
    }

    @Override
    public Logger getLogger() {
      return log;
    }

    @Override
    public void put(ExecutionContext executionContext) {
      executionContexts.add(executionContext);
      if (onPutShouldClose.test(executionContext)) {
        close();
      }
    }

    @Override
    public ExecutionContext take() {
      throw new UnsupportedOperationException();
    }
  }

  @Test
  public void invalidQueuedOperationFails() throws Exception {
    Poller poller = mock(Poller.class);

    Operation badOperation = Operation.newBuilder().setName("bad").build();
    QueueEntry badEntry =
        QueueEntry.newBuilder()
            .setExecuteEntry(ExecuteEntry.newBuilder().setOperationName(badOperation.getName()))
            .build();

    WorkerContext workerContext = mock(WorkerContext.class);
    when(workerContext.isInputFetching()).thenReturn(true);
    when(workerContext.getInputFetchStageWidth()).thenReturn(1);
    when(workerContext.getInputFetchDeadline()).thenReturn(60);
    // inspire empty argument list in Command resulting in null
    when(workerContext.getQueuedOperation(badEntry))
        .thenReturn(QueuedOperation.getDefaultInstance());
    when(workerContext.putOperation(any(Operation.class))).thenReturn(true);

    PipelineSink sinkOutput = new PipelineSink((executionContext) -> false);
    PipelineSink error =
        new PipelineSink((executionContext) -> true) {
          @Override
          public void put(ExecutionContext executionContext) {
            super.put(executionContext);
            sinkOutput.close();
          }
        };
    PipelineStage inputFetchStage = new InputFetchStage(workerContext, sinkOutput, error);
    ExecutionContext badContext =
        ExecutionContext.newBuilder()
            .setOperation(badOperation)
            .setPoller(poller)
            .setQueueEntry(badEntry)
            .build();
    inputFetchStage.claim(badContext);
    inputFetchStage.put(badContext);
    inputFetchStage.run();
    verify(poller, times(1)).pause();
    verify(workerContext, times(1)).getInputFetchStageWidth();
    verify(workerContext, times(1)).getInputFetchDeadline();
    verify(workerContext, times(1)).getQueuedOperation(badEntry);
    verify(workerContext, times(1)).putOperation(any(Operation.class));
    verify(workerContext, times(1))
        .resumePoller(
            eq(poller),
            eq("InputFetcher"),
            eq(badEntry),
            eq(QUEUED),
            any(Runnable.class),
            any(Deadline.class),
            any(Executor.class));
    verify(workerContext, times(2)).isInputFetching();
    verifyNoMoreInteractions(workerContext);
    ExecutionContext executionContext = error.getExecutionContexts().getFirst();
    assertThat(executionContext).isEqualTo(badContext);
  }
}
