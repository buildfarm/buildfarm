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

import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata;
import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;

public class InputFetchStage extends PipelineStage {
  private final BlockingQueue<OperationContext> queue;

  public InputFetchStage(WorkerContext workerContext, PipelineStage output, PipelineStage error) {
    super("InputFetchStage", workerContext, output, error);
    queue = new ArrayBlockingQueue<>(1);
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
    // yeah, the poll fail should probably do something here...
    Poller poller = workerContext.createPoller(
        "InputFetchStage",
        operationContext.operation.getName(),
        ExecuteOperationMetadata.Stage.QUEUED);

    long fetchStartAt = System.nanoTime();

    boolean success = true;
    try {
      workerContext.createActionRoot(
          operationContext.execDir,
          operationContext.directoriesIndex,
          operationContext.action);
    } catch (IOException e) {
      success = false;
    }

    poller.stop();

    if (!success) {
      workerContext.requeue(operationContext.operation);
      return null;
    }

    Duration fetchedIn = Durations.fromNanos(System.nanoTime() - fetchStartAt);

    return operationContext.toBuilder()
        .setFetchedIn(fetchedIn)
        .build();
  }
}
