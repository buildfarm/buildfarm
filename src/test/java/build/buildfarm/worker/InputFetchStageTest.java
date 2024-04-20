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

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import build.bazel.remote.execution.v2.ExecutionStage;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.Poller;
import build.buildfarm.v1test.ExecuteEntry;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.QueuedOperation;
import com.google.common.collect.Lists;
import io.grpc.Deadline;
import java.util.List;
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
    @Getter private final List<OperationContext> operationContexts = Lists.newArrayList();
    private final Predicate<OperationContext> onPutShouldClose;

    PipelineSink(Predicate<OperationContext> onPutShouldClose) {
      super("Sink", null, null, null);
      this.onPutShouldClose = onPutShouldClose;
    }

    @Override
    public Logger getLogger() {
      return log;
    }

    @Override
    public void put(OperationContext operationContext) {
      operationContexts.add(operationContext);
      if (onPutShouldClose.test(operationContext)) {
        close();
      }
    }

    @Override
    public OperationContext take() {
      throw new UnsupportedOperationException();
    }
  }

  @Test
  public void invalidQueuedOperationFails() throws InterruptedException {
    Poller poller = mock(Poller.class);

    QueueEntry badEntry =
        QueueEntry.newBuilder()
            .setExecuteEntry(ExecuteEntry.newBuilder().setOperationName("bad"))
            .build();

    WorkerContext workerContext =
        new StubWorkerContext() {
          @Override
          public DigestUtil getDigestUtil() {
            return null;
          }

          @Override
          public void resumePoller(
              Poller poller,
              String name,
              QueueEntry queueEntry,
              ExecutionStage.Value stage,
              Runnable onFailure,
              Deadline deadline) {}

          @Override
          public int getInputFetchStageWidth() {
            return 1;
          }

          @Override
          public int getInputFetchDeadline() {
            return 60;
          }

          @Override
          public QueuedOperation getQueuedOperation(QueueEntry queueEntry) {
            assertThat(queueEntry).isEqualTo(badEntry);
            // inspire empty argument list in Command resulting in null
            return QueuedOperation.getDefaultInstance();
          }
        };

    PipelineSink sinkOutput = new PipelineSink((operationContext) -> false);
    PipelineSink error =
        new PipelineSink((operationContext) -> true) {
          @Override
          public void put(OperationContext operationContext) {
            super.put(operationContext);
            sinkOutput.close();
          }
        };
    PipelineStage inputFetchStage = new InputFetchStage(workerContext, sinkOutput, error);
    OperationContext badContext =
        OperationContext.newBuilder().setPoller(poller).setQueueEntry(badEntry).build();
    inputFetchStage.claim(badContext);
    inputFetchStage.put(badContext);
    inputFetchStage.run();
    verify(poller, times(1)).pause();
    assertThat(error.getOperationContexts().size()).isEqualTo(1);
    OperationContext operationContext = error.getOperationContexts().get(0);
    assertThat(operationContext).isEqualTo(badContext);
  }
}
