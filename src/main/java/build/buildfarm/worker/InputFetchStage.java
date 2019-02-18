// Copyright 2017 The Bazel Authors. All rights reserved.
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

import static java.util.logging.Level.WARNING;

import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Logger;

public class InputFetchStage extends PipelineStage {
  private static final Logger logger = Logger.getLogger(InputFetchStage.class.getName());

  private final BlockingQueue<OperationContext> queue;

  public InputFetchStage(WorkerContext workerContext, PipelineStage output, PipelineStage error) {
    super("InputFetchStage", workerContext, output, error);
    queue = new ArrayBlockingQueue<>(1);
  }

  @Override
  protected Logger getLogger() {
    return logger;
  }

  @Override
  public OperationContext take() throws InterruptedException {
    return queue.take();
  }

  @Override
  public void put(OperationContext operationContext) throws InterruptedException {
    queue.put(operationContext);
  }

  @Override
  protected OperationContext tick(OperationContext operationContext) throws InterruptedException {
    Poller poller = workerContext.createPoller(
        "InputFetchStage",
        operationContext.operation.getName(),
        ExecuteOperationMetadata.Stage.QUEUED);

    boolean success = true;
    try {
      workerContext.createActionRoot(operationContext.execDir, operationContext.action, operationContext.command);
    } catch (IOException e) {
      logger.log(WARNING, "could not create action root", e);
      success = false;
    }

    poller.stop();

    return success ? operationContext : null;
  }
}
