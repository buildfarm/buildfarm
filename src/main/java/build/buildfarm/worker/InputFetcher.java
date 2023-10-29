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
import build.buildfarm.common.OperationFailer;
import build.buildfarm.common.ProxyDirectoriesIndex;
import build.buildfarm.v1test.ExecuteEntry;
import build.buildfarm.v1test.QueuedOperation;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.google.longrunning.Operation;
import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import com.google.rpc.Code;
import com.google.rpc.Status;
import io.grpc.Deadline;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import javax.annotation.Nullable;
import lombok.extern.java.Log;

@Log
public class InputFetcher implements Runnable {
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

  private List<String> validateQueuedOperation(QueuedOperation queuedOperation) {
    // Capture a list of all validation failures on the queued operation.
    // A successful validation is a an empty list of failures.
    List<String> constraintFailures = new ArrayList<>();

    if (queuedOperation == null) {
      constraintFailures.add("QueuedOperation is missing.");
      return constraintFailures;
    }

    // Ensure the timeout is not too long by comparing it to the maximum allowed timeout
    Action action = queuedOperation.getAction();
    if (action.hasTimeout() && workerContext.hasMaximumActionTimeout()) {
      Duration timeout = action.getTimeout();
      Duration maximum = workerContext.getMaximumActionTimeout();
      if (Durations.compare(timeout, maximum) > 0) {
        constraintFailures.add(
            String.format(
                "Timeout is too long (%s > %s).", timeout.getSeconds(), maximum.getSeconds()));
      }
    }

    if (queuedOperation.getCommand().getArgumentsList().isEmpty()) {
      constraintFailures.add("Argument list is empty.");
    }

    return constraintFailures;
  }

  private long runInterruptibly(Stopwatch stopwatch) throws InterruptedException {
    final Thread fetcherThread = Thread.currentThread();
    workerContext.resumePoller(
        operationContext.poller,
        "InputFetcher",
        operationContext.queueEntry,
        QUEUED,
        fetcherThread::interrupt,
        Deadline.after(workerContext.getInputFetchDeadline(), SECONDS));
    try {
      return fetchPolled(stopwatch);
    } finally {
      operationContext.poller.pause();
    }
  }

  private static final String BAZEL_HOST_BIN_PREFIX = "bazel-out/host/bin/";
  private static final String BAZEL_RUNFILES_SUFFIX = ".runfiles/__main__/";

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
    if (!programDigest.equals(runfilesProgramDigest)) {
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

  @VisibleForTesting
  long fetchPolled(Stopwatch stopwatch) throws InterruptedException {
    String operationName = operationContext.queueEntry.getExecuteEntry().getOperationName();
    log.log(Level.FINER, format("fetching inputs: %s", operationName));

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
      List<String> constraintFailures = validateQueuedOperation(queuedOperation);
      if (!constraintFailures.isEmpty()) {
        log.log(
            Level.SEVERE,
            format("invalid queued operation: %s", String.join(" ", constraintFailures)));
        owner.error().put(operationContext);
        return 0;
      }

      directoriesIndex = new ProxyDirectoriesIndex(queuedOperation.getTree().getDirectoriesMap());

      execDir =
          workerContext.createExecDir(
              operationName,
              directoriesIndex,
              queuedOperation.getAction(),
              queuedOperation.getCommand());
    } catch (IOException e) {
      Status.Builder status = Status.newBuilder().setMessage("Error creating exec dir");
      if (e instanceof ExecDirException) {
        ExecDirException execDirEx = (ExecDirException) e;
        execDirEx.toStatus(status);
      } else {
        status.setCode(Code.INTERNAL.getNumber());
        log.log(Level.SEVERE, format("error creating exec dir for %s", operationName), e);
      }
      failOperation(status.build());
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
          log.log(
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
      log.log(Level.FINER, "InputFetcher: Operation " + operationName + " Failed to claim output");

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
        log.log(Level.SEVERE, format("interrupted while erroring %s", operationName), errorEx);
      } finally {
        Thread.currentThread().interrupt();
      }
    } catch (Exception e) {
      log.log(Level.WARNING, format("error while fetching inputs: %s", operationName), e);
      try {
        owner.error().put(operationContext);
      } catch (InterruptedException errorEx) {
        log.log(Level.SEVERE, format("interrupted while erroring %s", operationName), errorEx);
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

  private void failOperation(Status status) throws InterruptedException {
    ExecuteEntry executeEntry = operationContext.queueEntry.getExecuteEntry();
    Operation failedOperation =
        OperationFailer.get(operationContext.operation, executeEntry, status);

    try {
      workerContext.putOperation(failedOperation);
      OperationContext newOperationContext =
          operationContext.toBuilder().setOperation(failedOperation).build();
      owner.error().put(newOperationContext);
    } catch (Exception e) {
      String operationName = executeEntry.getOperationName();
      log.log(Level.SEVERE, format("Cannot report failed operation %s", operationName), e);
    }
  }
}
