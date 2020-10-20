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
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.ExecutedActionMetadata;
import build.bazel.remote.execution.v2.FileNode;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.v1test.QueuedOperation;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.google.protobuf.Duration;
import com.google.protobuf.util.Timestamps;
import io.grpc.Deadline;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

public class InputFetcher implements Runnable {
  private static final Logger logger = Logger.getLogger(InputFetcher.class.getName());

  private final WorkerContext workerContext;
  private final OperationContext operationContext;
  private final InputFetchStage owner;
  private boolean success = false;

  InputFetcher(
      WorkerContext workerContext, OperationContext operationContext, InputFetchStage owner) {
    this.workerContext = workerContext;
    this.operationContext = operationContext;
    this.owner = owner;
  }

  private boolean isQueuedOperationValid(QueuedOperation queuedOperation) {
    Action action = queuedOperation.getAction();

    if (action.hasTimeout() && workerContext.hasMaximumActionTimeout()) {
      Duration timeout = action.getTimeout();
      Duration maximum = workerContext.getMaximumActionTimeout();
      if (timeout.getSeconds() > maximum.getSeconds()
          || (timeout.getSeconds() == maximum.getSeconds()
              && timeout.getNanos() > maximum.getNanos())) {
        return false;
      }
    }

    return !queuedOperation.getCommand().getArgumentsList().isEmpty();
  }

  private long runInterruptibly(Stopwatch stopwatch) throws InterruptedException {
    final Thread fetcherThread = Thread.currentThread();
    workerContext.resumePoller(
        operationContext.poller,
        "InputFetcher",
        operationContext.queueEntry,
        QUEUED,
        () -> fetcherThread.interrupt(),
        Deadline.after(60, SECONDS));
    try {
      return fetchPolled(stopwatch);
    } finally {
      operationContext.poller.pause();
    }
  }

  private static String BAZEL_HOST_BIN_PREFIX = "bazel-out/host/bin/";
  private static String BAZEL_RUNFILES_SUFFIX = ".runfiles/__main__/";

  static String getExecutablePath(
      String programPath, Directory root, Map<Digest, Directory> directoriesIndex) {
    if (!programPath.startsWith(BAZEL_HOST_BIN_PREFIX)) {
      return programPath;
    }
    Digest programDigest = pathDigest(programPath, root, directoriesIndex);
    if (programDigest == null) {
      return programPath;
    }
    String runfilesProgramPath =
        programPath + BAZEL_RUNFILES_SUFFIX + programPath.substring(BAZEL_HOST_BIN_PREFIX.length());
    Digest runfilesProgramDigest = pathDigest(runfilesProgramPath, root, directoriesIndex);
    if (runfilesProgramDigest == null) {
      return programPath;
    }
    if (!programDigest.equals(runfilesProgramPath)) {
      return programPath;
    }
    return runfilesProgramPath;
  }

  static @Nullable Digest pathDigest(
      String path, Directory root, Map<Digest, Directory> directoriesIndex) {
    Directory directory = root;
    String remaining = path;
    for (int index = remaining.indexOf('/'); index != -1; index = remaining.indexOf('/')) {
      String component = remaining.substring(index);
      Directory subdirectory = null;
      for (DirectoryNode node : directory.getDirectoriesList()) {
        if (component.equals(node.getName())) {
          subdirectory = directoriesIndex.get(node.getDigest());
        }
      }
      if (subdirectory == null) {
        return null;
      }
      while (index < remaining.length() && remaining.charAt(index) == '/') {
        index++;
      }
      directory = subdirectory;
      remaining = remaining.substring(index);
    }
    if (remaining.isEmpty()) {
      return null;
    }
    for (FileNode node : directory.getFilesList()) {
      if (node.getIsExecutable() && remaining.equals(node.getName())) {
        return node.getDigest();
      }
    }
    return null;
  }

  private long fetchPolled(Stopwatch stopwatch) throws InterruptedException {
    String operationName = operationContext.queueEntry.getExecuteEntry().getOperationName();
    logger.log(Level.INFO, format("fetching inputs: %s", operationName));

    ExecutedActionMetadata.Builder executedAction =
        operationContext
            .executeResponse
            .getResultBuilder()
            .getExecutionMetadataBuilder()
            .setInputFetchStartTimestamp(Timestamps.fromMillis(System.currentTimeMillis()));

    final Map<Digest, Directory> directoriesIndex;
    QueuedOperation queuedOperation;
    Path execDir;
    try {
      queuedOperation = workerContext.getQueuedOperation(operationContext.queueEntry);
      if (queuedOperation == null || !isQueuedOperationValid(queuedOperation)) {
        logger.log(Level.SEVERE, format("invalid queued operation: %s", operationName));
        owner.error().put(operationContext);
        return 0;
      }

      if (queuedOperation.hasTree()) {
        directoriesIndex =
            DigestUtil.proxyDirectoriesIndex(queuedOperation.getTree().getDirectories());
      } else {
        // TODO remove legacy interpretation and field after transition
        directoriesIndex =
            workerContext.getDigestUtil().createDirectoriesIndex(queuedOperation.getLegacyTree());
      }

      execDir =
          workerContext.createExecDir(
              operationName,
              directoriesIndex,
              queuedOperation.getAction(),
              queuedOperation.getCommand());
    } catch (IOException e) {
      logger.log(Level.SEVERE, format("error creating exec dir for %s", operationName), e);
      owner.error().put(operationContext);
      return 0;
    }
    success = true;

    /* tweak command executable used */
    String programName = queuedOperation.getCommand().getArguments(0);
    Directory root = directoriesIndex.get(queuedOperation.getTree().getRootDigest());
    Command command =
        queuedOperation
            .getCommand()
            .toBuilder()
            .clearArguments()
            .addArguments(getExecutablePath(programName, root, directoriesIndex))
            .addAllArguments(Iterables.skip(queuedOperation.getCommand().getArgumentsList(), 1))
            .build();

    executedAction.setInputFetchCompletedTimestamp(
        Timestamps.fromMillis(System.currentTimeMillis()));

    // we are now responsible for destroying the exec dir if anything goes wrong
    boolean completed = false;
    try {
      long fetchUSecs = stopwatch.elapsed(MICROSECONDS);
      proceedToOutput(queuedOperation.getAction(), command, execDir);
      completed = true;
      return stopwatch.elapsed(MICROSECONDS) - fetchUSecs;
    } finally {
      if (!completed) {
        try {
          workerContext.destroyExecDir(execDir);
        } catch (IOException e) {
          logger.log(
              Level.SEVERE,
              format("error deleting exec dir for %s after interrupt", operationName));
        }
      }
    }
  }

  private void proceedToOutput(Action action, Command command, Path execDir)
      throws InterruptedException {
    // switch poller to disable deadline
    operationContext.poller.pause();
    workerContext.resumePoller(
        operationContext.poller,
        "InputFetcher(claim)",
        operationContext.queueEntry,
        QUEUED,
        () -> {},
        Deadline.after(10, DAYS));

    OperationContext fetchedOperationContext =
        operationContext
            .toBuilder()
            .setExecDir(execDir)
            .setAction(action)
            .setCommand(command)
            .build();
    boolean claimed = owner.output().claim(fetchedOperationContext);
    operationContext.poller.pause();
    if (claimed) {
      try {
        owner.output().put(fetchedOperationContext);
      } catch (InterruptedException e) {
        owner.output().release();
        throw e;
      }
    } else {
      String operationName = operationContext.queueEntry.getExecuteEntry().getOperationName();
      logger.log(
          Level.INFO, "InputFetcher: Operation " + operationName + " Failed to claim output");

      owner.error().put(operationContext);
    }
  }

  @Override
  public void run() {
    long stallUSecs = 0;
    String operationName = operationContext.queueEntry.getExecuteEntry().getOperationName();
    Stopwatch stopwatch = Stopwatch.createStarted();
    try {
      stallUSecs = runInterruptibly(stopwatch);
    } catch (InterruptedException e) {
      /* we can be interrupted when the poller fails */
      try {
        owner.error().put(operationContext);
      } catch (InterruptedException errorEx) {
        logger.log(Level.SEVERE, format("interrupted while erroring %s", operationName), errorEx);
      } finally {
        Thread.currentThread().interrupt();
      }
    } catch (Exception e) {
      logger.log(Level.WARNING, format("error while fetching inputs: %s", operationName), e);
      try {
        owner.error().put(operationContext);
      } catch (InterruptedException errorEx) {
        logger.log(Level.SEVERE, format("interrupted while erroring %s", operationName), errorEx);
      }
      throw e;
    } finally {
      boolean wasInterrupted = Thread.interrupted();
      // allow release to occur without interrupted state
      try {
        owner.releaseInputFetcher(
            operationName, stopwatch.elapsed(MICROSECONDS), stallUSecs, success);
      } finally {
        if (wasInterrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }
}
