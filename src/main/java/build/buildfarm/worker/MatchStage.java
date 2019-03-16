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
import build.buildfarm.common.DigestUtil;
import build.buildfarm.instance.Instance.MatchListener;
import build.buildfarm.v1test.QueuedOperationMetadata;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.base.Stopwatch;
import com.google.longrunning.Operation;
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
    public boolean onOperationName(String operationName) {
      operationNamedAtUSecs = stopwatch.elapsed(MICROSECONDS);
      Preconditions.checkState(poller == null);
      poller = workerContext.createPoller(
          "MatchStage",
          operationName,
          ExecuteOperationMetadata.Stage.QUEUED);
      return true;
    }

    @Override
    public boolean onOperation(Operation operation) {
      try {
        return onOperationPolled(operation);
      } finally {
        if (!Thread.currentThread().isInterrupted() && poller != null) {
          poller.stop();
          poller = null;
        }
      }
    }

    private boolean onOperationPolled(Operation operation) {
      if (operation == null) {
        output.release();
        return false;
      }

      logStart(operation.getName());

      try {
        long matchingAtUSecs = stopwatch.elapsed(MICROSECONDS);
        OperationContext context = match(operation, operationNamedAtUSecs);
        long matchedInUSecs = stopwatch.elapsed(MICROSECONDS) - matchingAtUSecs;
        logComplete(operation.getName(), matchedInUSecs, waitDuration, context != null);
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

  private OperationContext match(Operation operation, long matchStartAt) throws InterruptedException {
    if (!operation.getMetadata().is(QueuedOperationMetadata.class)) {
      return null;
    }

    QueuedOperationMetadata metadata;
    try {
      metadata = operation.getMetadata().unpack(QueuedOperationMetadata.class);
    } catch (InvalidProtocolBufferException e) {
      logger.log(SEVERE, "error unpacking queued operation metadata for " + operation.getName(), e);
      return null;
    }

    Action action = metadata.getAction();

    if (action.hasTimeout() && workerContext.hasMaximumActionTimeout()) {
      Duration timeout = action.getTimeout();
      Duration maximum = workerContext.getMaximumActionTimeout();
      if (timeout.getSeconds() > maximum.getSeconds() ||
          (timeout.getSeconds() == maximum.getSeconds() && timeout.getNanos() > maximum.getNanos())) {
        return null;
      }
    }

    Command command = metadata.getCommand();
    if (command.getArgumentsList().isEmpty()) {
      return null;
    }

    OperationContext.Builder builder = OperationContext.newBuilder()
        .setOperation(operation)
        .setDirectoriesIndex(createDirectoriesIndex(metadata.getDirectoriesList(), workerContext.getDigestUtil()))
        .setMetadata(metadata.getExecuteOperationMetadata())
        .setAction(action)
        .setCommand(command)
        .setExecutionPolicy(metadata.getExecutionPolicy())
        .setResultsCachePolicy(metadata.getResultsCachePolicy())
        .setRequestMetadata(metadata.getRequestMetadata());

    Duration matchedIn = Durations.fromNanos(System.nanoTime() - matchStartAt);
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
