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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;
import lombok.extern.java.Log;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Log
public class PipelineTest {
  abstract static class AbstractPipelineStage extends PipelineStage {
    public AbstractPipelineStage(String name) {
      super(name, null, null, null);
    }

    @Override
    public Logger getLogger() {
      return log;
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
  public void stageThreadReturnCompletesJoin() throws InterruptedException {
    Pipeline pipeline = new Pipeline();
    pipeline.add(
        new AbstractPipelineStage("returner") {
          @Override
          public void run() {}
        },
        1);
    pipeline.start();
    pipeline.join();
  }

  @Test
  public void stageThreadExceptionCompletesJoin() throws InterruptedException {
    Pipeline pipeline = new Pipeline();
    pipeline.add(
        new AbstractPipelineStage("exceptioner") {
          @Override
          public void run() {
            throw new RuntimeException("uncaught");
          }
        },
        1);
    pipeline.start();
    pipeline.join();
  }

  // Create a test stage that exists because of an interrupt.
  // This proves the stage can be interupted.
  @SuppressWarnings("PMD.TestClassWithoutTestCases")
  public class TestStage extends PipelineStage {
    public TestStage(String name) {
      super(name, null, null, null);
    }

    @Override
    protected void runInterruptible() throws InterruptedException {
      throw new InterruptedException("Interrupt");
    }

    @Override
    public void put(OperationContext operationContext) throws InterruptedException {}

    @Override
    OperationContext take() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Logger getLogger() {
      return log;
    }
  }

  // This test demonstrates that the stage will end and the pipeline will finish because it was
  // interrupted.
  @Test
  public void stageExitsOnInterrupt() throws InterruptedException {
    Pipeline pipeline = new Pipeline();
    TestStage stage = new TestStage("test");
    pipeline.add(stage, 1);
    pipeline.start();
    pipeline.join();
  }

  // Create a test stage that doesn't exit because of an a non-interrupt exception.
  // This proves the stage is robust enough to continue running when experiencing an exception.
  public class ContinueStage extends PipelineStage {
    private static PipelineStage OPEN_STAGE =
        new AbstractPipelineStage("open") {
          @Override
          public void run() {}
        };

    private boolean first = true;

    public ContinueStage(String name) {
      super(name, /* workerContext= */ null, /* output= */ OPEN_STAGE, /* error= */ null);
    }

    @Override
    protected void iterate() throws InterruptedException {
      if (first) {
        first = false;
        throw new RuntimeException("Exception");
      }
      // avoid pouring errors into test log, a single exception is enough
    }

    @Override
    public void put(OperationContext operationContext) throws InterruptedException {}

    @Override
    OperationContext take() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Logger getLogger() {
      return log;
    }
  }

  // This test demonstrates that the stage will NOT end and the pipeline will NOT finish because a
  // non-interrupt exception was thrown.
  @Test
  public void stageContinuesOnException() throws InterruptedException {
    Pipeline pipeline = new Pipeline();
    ContinueStage stage = new ContinueStage("test");
    pipeline.add(stage, 1);
    pipeline.start();

    boolean didNotThrow = false;
    try {
      CompletableFuture.runAsync(
              () -> {
                try {
                  pipeline.join();
                } catch (InterruptedException e) {
                }
                return;
              })
          .get(1, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      didNotThrow = true;
    } catch (Exception e) {
    }
    assertThat(didNotThrow).isTrue();
  }
}
