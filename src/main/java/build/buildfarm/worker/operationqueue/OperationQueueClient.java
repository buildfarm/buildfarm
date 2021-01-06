// Copyright 2020 The Bazel Authors. All rights reserved.
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

package build.buildfarm.worker.operationqueue;

import static java.util.logging.Level.SEVERE;

import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecutionStage;
import build.bazel.remote.execution.v2.Platform;
import build.buildfarm.common.Write;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.Instance.MatchListener;
import build.buildfarm.v1test.ExecuteEntry;
import build.buildfarm.v1test.ExecutionPolicy;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.QueuedOperationMetadata;
import build.buildfarm.worker.ExecutionPolicies;
import build.buildfarm.worker.RetryingMatchListener;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.logging.Logger;
import javax.annotation.Nullable;

class OperationQueueClient {
  private static final Logger logger = Logger.getLogger(Worker.class.getName());

  private final Instance instance;
  private final Platform matchPlatform;
  private final Set<String> activeOperations = new ConcurrentSkipListSet<>();

  OperationQueueClient(Instance instance, Platform platform, Iterable<ExecutionPolicy> policies) {
    this.instance = instance;
    matchPlatform = ExecutionPolicies.getMatchPlatform(platform, policies);
  }

  void match(MatchListener listener) throws InterruptedException {
    RetryingMatchListener dedupMatchListener =
        new RetryingMatchListener() {
          boolean matched = false;

          @Override
          public boolean getMatched() {
            return matched;
          }

          @Override
          public void onWaitStart() {
            listener.onWaitStart();
          }

          @Override
          public void onWaitEnd() {
            listener.onWaitEnd();
          }

          @Override
          public boolean onEntry(@Nullable QueueEntry queueEntry) throws InterruptedException {
            if (queueEntry == null) {
              matched = true;
              return listener.onEntry(null);
            }
            ExecuteEntry executeEntry = queueEntry.getExecuteEntry();
            String operationName = executeEntry.getOperationName();
            if (activeOperations.contains(operationName)) {
              logger.log(
                  SEVERE,
                  "WorkerContext::match: WARNING matched duplicate operation " + operationName);
              return false;
            }
            matched = true;
            activeOperations.add(operationName);
            boolean success = listener.onEntry(queueEntry);
            if (!success) {
              requeue(
                  Operation.newBuilder()
                      .setName(operationName)
                      .setMetadata(
                          Any.pack(
                              QueuedOperationMetadata.newBuilder()
                                  .setExecuteOperationMetadata(
                                      ExecuteOperationMetadata.newBuilder()
                                          .setActionDigest(executeEntry.getActionDigest())
                                          .setStdoutStreamName(executeEntry.getStdoutStreamName())
                                          .setStderrStreamName(executeEntry.getStderrStreamName())
                                          .setStage(ExecutionStage.Value.QUEUED))
                                  .setQueuedOperationDigest(queueEntry.getQueuedOperationDigest())
                                  .build()))
                      .build());
            }
            return success;
          }

          @Override
          public void onError(Throwable t) {
            listener.onError(t);
          }

          @Override
          public void setOnCancelHandler(Runnable onCancelHandler) {
            listener.setOnCancelHandler(onCancelHandler);
          }
        };
    while (!dedupMatchListener.getMatched()) {
      instance.match(matchPlatform, dedupMatchListener);
    }
  }

  void requeue(Operation operation) throws InterruptedException {
    try {
      deactivate(operation.getName());
      ExecuteOperationMetadata metadata =
          operation.getMetadata().unpack(ExecuteOperationMetadata.class);

      ExecuteOperationMetadata executingMetadata =
          metadata.toBuilder().setStage(ExecutionStage.Value.QUEUED).build();

      operation = operation.toBuilder().setMetadata(Any.pack(executingMetadata)).build();
      instance.putOperation(operation);
    } catch (InvalidProtocolBufferException e) {
      logger.log(
          SEVERE, "error unpacking execute operation metadata for " + operation.getName(), e);
    }
  }

  void deactivate(String name) {
    activeOperations.remove(name);
  }

  boolean put(Operation operation) throws InterruptedException {
    return instance.putOperation(operation);
  }

  public Write getStreamWrite(String name) {
    return instance.getOperationStreamWrite(name);
  }

  public boolean poll(String name, ExecutionStage.Value stage) {
    return instance.pollOperation(name, stage);
  }
}
