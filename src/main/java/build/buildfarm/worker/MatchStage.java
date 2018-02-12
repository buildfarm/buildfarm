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
import com.google.longrunning.Operation;
import com.google.protobuf.InvalidProtocolBufferException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

class MatchStage extends PipelineStage {
  MatchStage(Worker worker, PipelineStage output, PipelineStage error) {
    super(worker, output, error);
  }

  @Override
  protected void iterate() throws InterruptedException {
    if (!output.claim()) {
      return;
    }
    worker.instance.match(worker.config.getPlatform(), worker.config.getRequeueOnFailure(), (operation) -> {
      return fetch(operation);
    });
  }

  private boolean fetch(Operation operation) {
    ExecuteOperationMetadata metadata;
    Action action;
    try {
      metadata = operation.getMetadata().unpack(ExecuteOperationMetadata.class);
      action = Action.parseFrom(worker.instance.getBlob(metadata.getActionDigest()));
    } catch (InvalidProtocolBufferException ex) {
      return false;
    }
    Path execDir = worker.root.resolve(operation.getName());
    output.offer(new OperationContext(
        operation,
        execDir,
        metadata,
        action,
        new ArrayList<>(),
        new ArrayList<>()));
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
  public void offer(OperationContext operation) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setInput(PipelineStage input) {
    throw new UnsupportedOperationException();
  }
}
