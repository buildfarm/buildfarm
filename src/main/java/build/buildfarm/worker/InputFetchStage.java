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

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.logging.Level.SEVERE;

import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;
import io.grpc.Deadline;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

public class InputFetchStage extends PipelineStage {
  private static final Logger logger = Logger.getLogger(InputFetchStage.class.getName());

  private final BlockingQueue<OperationContext> queue = new ArrayBlockingQueue<>(1);

  public InputFetchStage(WorkerContext workerContext, PipelineStage output, PipelineStage error) {
    super("InputFetchStage", workerContext, output, error);
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
    String operationName = operationContext.operation.getName();
    Poller poller = workerContext.createPoller(
        "InputFetchStage",
        operationName,
        ExecuteOperationMetadata.Stage.QUEUED,
        this::cancelTick,
        Deadline.after(60, SECONDS));

    workerContext.logInfo("InputFetchStage: Fetching inputs: " + operationContext.operation.getName());

    long fetchStartAt = System.nanoTime();

    boolean success = true;
    Path execDir;
    try {
      execDir = workerContext.createExecDir(
          operationName,
          operationContext.directoriesIndex,
          operationContext.action,
          operationContext.command);
    } catch (IOException e) {
      logger.log(SEVERE, "error creating exec dir for " + operationName, e);
      return null;
    } finally {
      poller.stop();
    }

    Duration fetchedIn = Durations.fromNanos(System.nanoTime() - fetchStartAt);

    return operationContext.toBuilder()
        .setExecDir(execDir)
        .setFetchedIn(fetchedIn)
        .build();
  }
}
