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

import com.google.longrunning.Operation;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PipelineStageTest {
  abstract static class AbstractPipelineStage extends PipelineStage {
    public AbstractPipelineStage(String name) {
      this(name, null, null, null);
    }

    public AbstractPipelineStage(String name, WorkerContext workerContext, PipelineStage output, PipelineStage error) {
      super(name, workerContext, output, error);
    }

    @Override
    public void put(OperationContext operationContext) {
      throw new UnsupportedOperationException();
    }

    @Override
    OperationContext take() {
      throw new UnsupportedOperationException();
    }
  }

  @Test
  public void cancelTickInterruptsOperation() throws InterruptedException {
    PipelineStage output = new AbstractPipelineStage("singleOutput") {
      @Override
      public void put(OperationContext operationContext) {
        close();
      }
    };
    AtomicInteger errorCount = new AtomicInteger();
    PipelineStage error = new AbstractPipelineStage("error") {
      @Override
      public void put(OperationContext operationContext) {
        errorCount.getAndIncrement();
      }
    };
    OperationContext operationContext = OperationContext.newBuilder()
        .setOperation(Operation.getDefaultInstance())
        .build();
    AtomicInteger count = new AtomicInteger();
    PipelineStage stage = new AbstractPipelineStage("waiter", new StubWorkerContext(), output, error) {
      Object lock = new Object();

      @Override
      public OperationContext tick(OperationContext operationContext) throws InterruptedException {
        count.getAndIncrement();
        if (count.get() == 1) {
          synchronized (lock) {
            lock.wait();
          }
        }
        return operationContext;
      }

      @Override
      OperationContext take() {
        return operationContext;
      }
    };
    Thread stageThread = new Thread(stage);
    stageThread.start();
    while (count.get() != 1);
    stage.cancelTick();
    stageThread.join();
    assertThat(count.get()).isEqualTo(2);
    assertThat(errorCount.get()).isEqualTo(1);
  }
}
