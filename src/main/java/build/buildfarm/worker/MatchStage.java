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

import static build.bazel.remote.execution.v2.ExecutionStage.Value.QUEUED;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.buildfarm.common.Poller;
import build.buildfarm.instance.MatchListener;
import build.buildfarm.v1test.ExecuteEntry;
import build.buildfarm.v1test.QueueEntry;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import lombok.extern.java.Log;

@Log
public class MatchStage extends PipelineStage {
  private boolean inGracefulShutdown = false;

  public MatchStage(WorkerContext workerContext, PipelineStage output, PipelineStage error) {
    super("MatchStage", workerContext, output, error);
  }

  class MatchOperationListener implements MatchListener {
    private OperationContext operationContext;
    private final Stopwatch stopwatch;
    private long waitStart;
    private long waitDuration;
    private Poller poller = null;
    private QueueEntry queueEntry = null;
    private boolean matched = false;

    public MatchOperationListener(OperationContext operationContext, Stopwatch stopwatch) {
      this.operationContext = operationContext;
      this.stopwatch = stopwatch;
      waitDuration = this.stopwatch.elapsed(MICROSECONDS);
    }

    boolean wasMatched() {
      return matched;
    }

    @Override
    public void onWaitStart() {
      waitStart = stopwatch.elapsed(MICROSECONDS);
    }

    @Override
    public void onWaitEnd() {
      long elapsedUSecs = stopwatch.elapsed(MICROSECONDS);
      waitDuration += elapsedUSecs - waitStart;
      waitStart = elapsedUSecs;
    }

    @SuppressWarnings("ConstantConditions")
    @Override
    public boolean onEntry(@Nullable QueueEntry queueEntry) throws InterruptedException {
      if (queueEntry == null) {
        return false;
      }

      Preconditions.checkState(poller == null);
      operationContext =
          operationContext
              .toBuilder()
              .setQueueEntry(queueEntry)
              .setPoller(workerContext.createPoller("MatchStage", queueEntry, QUEUED))
              .build();
      return onOperationPolled();
    }

    @Override
    public void onError(Throwable t) {
      Throwables.throwIfUnchecked(t);
      throw new RuntimeException(t);
    }

    @SuppressWarnings("SameReturnValue")
    private boolean onOperationPolled() throws InterruptedException {
      String operationName = operationContext.queueEntry.getExecuteEntry().getOperationName();
      start(operationName);

      long matchingAtUSecs = stopwatch.elapsed(MICROSECONDS);
      OperationContext matchedOperationContext = match(operationContext);
      long matchedInUSecs = stopwatch.elapsed(MICROSECONDS) - matchingAtUSecs;
      complete(operationName, matchedInUSecs, waitDuration, true);
      matchedOperationContext.poller.pause();
      try {
        output.put(matchedOperationContext);
      } catch (InterruptedException e) {
        error.put(matchedOperationContext);
        throw e;
      }
      matched = true;
      return true;
    }
  }

  @Override
  protected Logger getLogger() {
    return log;
  }

  @Override
  protected void iterate() throws InterruptedException {
    // stop matching and picking up any works if the worker is in graceful shutdown.
    if (inGracefulShutdown) {
      return;
    }
    Stopwatch stopwatch = Stopwatch.createStarted();
    OperationContext operationContext = OperationContext.newBuilder().build();
    if (!output.claim(operationContext)) {
      return;
    }
    MatchOperationListener listener = new MatchOperationListener(operationContext, stopwatch);
    try {
      start();
      workerContext.match(listener);
    } finally {
      if (!listener.wasMatched()) {
        output.release();
      }
    }
  }

  void prepareForGracefulShutdown() {
    inGracefulShutdown = true;
  }

  private OperationContext match(OperationContext operationContext) {
    Timestamp workerStartTimestamp = Timestamps.fromMillis(System.currentTimeMillis());

    ExecuteEntry executeEntry = operationContext.queueEntry.getExecuteEntry();
    // this may be superfluous - we can probably just set the name and action digest
    Operation operation =
        Operation.newBuilder()
            .setName(executeEntry.getOperationName())
            .setMetadata(
                Any.pack(
                    ExecuteOperationMetadata.newBuilder()
                        .setActionDigest(executeEntry.getActionDigest())
                        .setStage(QUEUED)
                        .setStdoutStreamName(executeEntry.getStdoutStreamName())
                        .setStderrStreamName(executeEntry.getStderrStreamName())
                        .build()))
            .build();

    OperationContext matchedOperationContext =
        operationContext.toBuilder().setOperation(operation).build();

    matchedOperationContext
        .executeResponse
        .getResultBuilder()
        .getExecutionMetadataBuilder()
        .setWorker(workerContext.getName())
        .setQueuedTimestamp(executeEntry.getQueuedTimestamp())
        .setWorkerStartTimestamp(workerStartTimestamp);
    return matchedOperationContext;
  }

  @Override
  public OperationContext take() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean claim(OperationContext operationContext) {
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
}
