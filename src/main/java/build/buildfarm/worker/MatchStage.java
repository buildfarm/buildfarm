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

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.logging.Level.SEVERE;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata.Stage;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.instance.Instance.MatchListener;
import build.buildfarm.v1test.ExecuteEntry;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.QueuedOperation;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.base.Stopwatch;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

public class MatchStage extends PipelineStage {
  private static final Logger logger = Logger.getLogger(MatchStage.class.getName());

  public MatchStage(WorkerContext workerContext, PipelineStage output, PipelineStage error) {
    super("MatchStage", workerContext, output, error);
  }

  class MatchOperationListener implements MatchListener {
    private Stopwatch stopwatch;
    private long waitStart;
    private long waitDuration;
    private long operationNamedAtUSecs;
    private Poller poller = null;
    private QueueEntry queueEntry = null;

    public MatchOperationListener(Stopwatch stopwatch) {
      this.stopwatch = stopwatch;
      waitDuration = this.stopwatch.elapsed(MICROSECONDS);
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

    @Override
    public boolean onEntry(QueueEntry queueEntry) {
      operationNamedAtUSecs = stopwatch.elapsed(MICROSECONDS);
      Preconditions.checkState(poller == null);
      poller = workerContext.createPoller(
          "MatchStage",
          queueEntry,
          Stage.QUEUED);
      this.queueEntry = queueEntry;
      return true;
    }

    @Override
    public boolean onOperation(QueuedOperation queuedOperation) {
      try {
        return onOperationPolled(queuedOperation);
      } finally {
        if (!Thread.currentThread().isInterrupted() && poller != null) {
          poller.stop();
          poller = null;
        }
      }
    }

    private boolean onOperationPolled(QueuedOperation queuedOperation) {
      if (queuedOperation == null) {
        output.release();
        return false;
      }

      String operationName = queueEntry.getExecuteEntry().getOperationName();
      logStart(operationName);

      try {
        long matchingAtUSecs = stopwatch.elapsed(MICROSECONDS);
        OperationContext context = match(
            queueEntry,
            queuedOperation,
            stopwatch,
            operationNamedAtUSecs);
        long matchedInUSecs = stopwatch.elapsed(MICROSECONDS) - matchingAtUSecs;
        logComplete(operationName, matchedInUSecs, waitDuration, context != null);
        if (context == null) {
          output.release();
        }
        if (poller != null) {
          poller.stop();
          poller = null;
        }
        if (context != null) {
          output.put(context);
        }
        return context != null;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
  }

  @Override
  protected Logger getLogger() {
    return logger;
  }

  @Override
  protected void iterate() throws InterruptedException {
    Stopwatch stopwatch = Stopwatch.createStarted();
    if (!output.claim()) {
      return;
    }
    MatchOperationListener listener = new MatchOperationListener(stopwatch);

    logStart();
    workerContext.match(listener);
  }

  private static Map<Digest, Directory> createDirectoriesIndex(Iterable<Directory> directories, DigestUtil digestUtil) {
    Set<Digest> directoryDigests = new HashSet<>();
    ImmutableMap.Builder<Digest, Directory> directoriesIndex = new ImmutableMap.Builder<>();
    for (Directory directory : directories) {
      // double compute here...
      Digest directoryDigest = digestUtil.compute(directory);
      if (!directoryDigests.add(directoryDigest)) {
        continue;
      }
      directoriesIndex.put(directoryDigest, directory);
    }

    return directoriesIndex.build();
  }

  private OperationContext match(
      QueueEntry queueEntry,
      QueuedOperation queuedOperation,
      Stopwatch stopwatch,
      long matchStartAtUSecs) throws InterruptedException {
    Action action = queuedOperation.getAction();

    if (action.hasTimeout() && workerContext.hasMaximumActionTimeout()) {
      Duration timeout = action.getTimeout();
      Duration maximum = workerContext.getMaximumActionTimeout();
      if (timeout.getSeconds() > maximum.getSeconds() ||
          (timeout.getSeconds() == maximum.getSeconds() && timeout.getNanos() > maximum.getNanos())) {
        return null;
      }
    }

    Command command = queuedOperation.getCommand();
    if (command.getArgumentsList().isEmpty()) {
      return null;
    }

    ExecuteEntry executeEntry = queueEntry.getExecuteEntry();
    // this may be superfluous - we can probably just set the name and action digest
    Operation operation = Operation.newBuilder()
        .setName(executeEntry.getOperationName())
        .setMetadata(Any.pack(ExecuteOperationMetadata.newBuilder()
            .setActionDigest(executeEntry.getActionDigest())
            .setStage(Stage.QUEUED)
            .setStdoutStreamName(executeEntry.getStdoutStreamName())
            .setStderrStreamName(executeEntry.getStderrStreamName())
            .build()))
        .build();

    OperationContext.Builder builder = OperationContext.newBuilder()
        .setOperation(operation)
        .setDirectoriesIndex(createDirectoriesIndex(queuedOperation.getDirectoriesList(), workerContext.getDigestUtil()))
        .setAction(action)
        .setCommand(command)
        .setQueueEntry(queueEntry);

    Duration matchedIn = Durations.fromMicros(stopwatch.elapsed(MICROSECONDS) - matchStartAtUSecs);
    return builder
        .setMatchedIn(matchedIn)
        .build();
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
