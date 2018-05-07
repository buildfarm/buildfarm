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

import com.google.devtools.remoteexecution.v1test.Action;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata;
import com.google.devtools.remoteexecution.v1test.Platform;
import com.google.longrunning.Operation;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class MatchStage extends PipelineStage {
  public MatchStage(WorkerContext workerContext, PipelineStage output, PipelineStage error) {
    super(workerContext, output, error);
  }

  @Override
  protected void iterate() throws InterruptedException {
    if (!output.claim()) {
      return;
    }
    workerContext.match((operation) -> {
      boolean fetched = fetch(operation);
      if (!fetched) {
        output.release();
      }
      return fetched;
    });
    // trigger stage shutdown if interrupted during fetch
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
  }

  private boolean fetch(Operation operation) {
    ExecuteOperationMetadata metadata;
    Action action;
    try {
      metadata = operation.getMetadata().unpack(ExecuteOperationMetadata.class);
    } catch (InvalidProtocolBufferException ex) {
      return false;
    }

    ByteString actionBlob = workerContext.getBlob(metadata.getActionDigest());
    if (actionBlob == null) {
      return false;
    }

    try {
      action = Action.parseFrom(actionBlob);
    } catch (InvalidProtocolBufferException ex) {
      return false;
    }

    if (action.hasTimeout() && workerContext.hasMaximumActionTimeout()) {
      Duration timeout = action.getTimeout();
      Duration maximum = workerContext.getMaximumActionTimeout();
      if (timeout.getSeconds() > maximum.getSeconds() ||
          (timeout.getSeconds() == maximum.getSeconds() && timeout.getNanos() > maximum.getNanos())) {
        return false;
      }
    }
    Path execDir = workerContext.getRoot().resolve(operation.getName());
    try {
      output.put(new OperationContext(
          operation,
          execDir,
          metadata,
          action));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
    return true;
  }

  @Override
  public OperationContext take() throws InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean claim() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void release() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void put(OperationContext operation) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setInput(PipelineStage input) {
    throw new UnsupportedOperationException();
  }
}
