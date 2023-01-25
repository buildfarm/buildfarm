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
}
