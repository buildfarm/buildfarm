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

import static build.bazel.remote.execution.v2.ExecutionStage.Value.COMPLETED;
import static build.bazel.remote.execution.v2.ExecutionStage.Value.EXECUTING;
import static build.buildfarm.common.Actions.asExecutionStatus;
import static build.buildfarm.common.Actions.isRetriable;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import build.bazel.remote.execution.v2.ExecuteResponse;
import build.bazel.remote.execution.v2.ExecutedActionMetadata;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.v1test.Digest;
import com.google.common.base.Stopwatch;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.google.rpc.Code;
import com.google.rpc.Status;
import io.grpc.Deadline;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.logging.Level;
import lombok.extern.java.Log;

@Log
class ResultReporter implements Runnable {
  private final WorkerContext workerContext;
  private final ExecutionContext executionContext;
  private final ReportResultStage owner;
  private boolean success = false;

  ResultReporter(
      WorkerContext workerContext, ExecutionContext executionContext, ReportResultStage owner) {
    this.workerContext = workerContext;
    this.executionContext = executionContext;
    this.owner = owner;
  }

  @Override
  public void run() {
    long stallUSecs = 0;
    String executionName = executionContext.queueEntry.getExecuteEntry().getOperationName();
    Stopwatch stopwatch = Stopwatch.createStarted();
    try {
      stallUSecs = runInterruptibly(stopwatch);
    } catch (InterruptedException e) {
      /* we can be interrupted when the poller fails */
      try {
        owner.error().put(executionContext);
      } catch (InterruptedException errorEx) {
        log.log(Level.SEVERE, format("interrupted while erroring %s", executionName), errorEx);
      } finally {
        Thread.currentThread().interrupt();
      }
    } catch (Exception e) {
      log.log(Level.WARNING, format("error while reporting results: %s", executionName), e);
      try {
        owner.error().put(executionContext);
      } catch (InterruptedException errorEx) {
        log.log(Level.SEVERE, format("interrupted while erroring %s", executionName), errorEx);
      }
      throw e;
    } finally {
      boolean wasInterrupted = Thread.interrupted();
      // allow release to occur without interrupted state
      try {
        owner.releaseResultReporter(
            executionName, stopwatch.elapsed(MICROSECONDS), stallUSecs, success);
        after(executionContext);
      } finally {
        if (wasInterrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  private long runInterruptibly(Stopwatch stopwatch) throws InterruptedException {
    workerContext.resumePoller(
        executionContext.poller,
        "ReportResultStage",
        executionContext.queueEntry,
        EXECUTING,
        Thread.currentThread()::interrupt,
        Deadline.after(60, SECONDS));
    try {
      return reportPolled(stopwatch);
    } finally {
      executionContext.poller.pause();
    }
  }

  private void putOperation(ExecutionContext executionContext) throws InterruptedException {
    Operation operation =
        executionContext.operation.toBuilder()
            .setMetadata(Any.pack(executionContext.metadata.build()))
            .build();

    boolean operationUpdateSuccess = false;
    try {
      operationUpdateSuccess = workerContext.putOperation(operation);
    } catch (IOException e) {
      log.log(Level.SEVERE, format("error putting operation %s", operation.getName()), e);
    }

    if (!operationUpdateSuccess) {
      log.log(
          Level.WARNING,
          String.format(
              "ReportResultStage::run(%s): could not record update", operation.getName()));
    }
  }

  private long reportPolled(Stopwatch stopwatch) throws InterruptedException {
    String operationName = executionContext.operation.getName();

    ExecutedActionMetadata.Builder executedAction =
        executionContext
            .metadata
            .getExecuteOperationMetadataBuilder()
            .getPartialExecutionMetadataBuilder()
            .setOutputUploadStartTimestamp(Timestamps.fromMillis(System.currentTimeMillis()));
    putOperation(executionContext);

    boolean blacklist = false;
    try {
      workerContext.uploadOutputs(
          executionContext.queueEntry.getExecuteEntry().getActionDigest(),
          executionContext.executeResponse.getResultBuilder(),
          executionContext.execDir,
          executionContext.command);
    } catch (StatusException | StatusRuntimeException e) {
      ExecuteResponse.Builder executeResponse = executionContext.executeResponse;
      if (executeResponse.getStatus().getCode() == Code.OK.getNumber()
          && executeResponse.getResult().getExitCode() == 0) {
        // something about the outputs was malformed - fail the operation with this status if not
        // already failing
        Status status = StatusProto.fromThrowable(e);
        if (status == null) {
          log.log(
              Level.SEVERE, String.format("no rpc status from exception for %s", operationName), e);
          status = asExecutionStatus(e);
        }
        executeResponse.setStatus(status);
        if (isRetriable(status)) {
          blacklist = true;
        }
      }
    } catch (InterruptedException | ClosedByInterruptException e) {
      // cancellation here should not be logged
      return 0;
    } catch (IOException e) {
      log.log(Level.SEVERE, String.format("error uploading outputs for %s", operationName), e);
      return 0;
    }

    Timestamp completed = Timestamps.now();
    executedAction
        .setWorkerCompletedTimestamp(completed)
        .setOutputUploadCompletedTimestamp(completed);

    executionContext.executeResponse.getResultBuilder().setExecutionMetadata(executedAction);
    // remove partial metadata in favor of result
    executionContext.metadata.getExecuteOperationMetadataBuilder().clearPartialExecutionMetadata();
    ExecuteResponse executeResponse = executionContext.executeResponse.build();

    if (blacklist
        || (!executionContext.action.getDoNotCache()
            && executeResponse.getStatus().getCode() == Code.OK.getNumber()
            && executeResponse.getResult().getExitCode() == 0)) {
      Digest actionDigest = executionContext.queueEntry.getExecuteEntry().getActionDigest();
      try {
        if (blacklist) {
          workerContext.blacklistAction(actionDigest.getHash());
        } else {
          workerContext.putActionResult(
              DigestUtil.asActionKey(actionDigest), executeResponse.getResult());
        }
      } catch (IOException e) {
        log.log(
            Level.SEVERE, String.format("error reporting action result for %s", operationName), e);
        return 0;
      }
    }

    executionContext.metadata.getExecuteOperationMetadataBuilder().setStage(COMPLETED);

    Operation completedOperation =
        executionContext.operation.toBuilder()
            .setDone(true)
            .setMetadata(Any.pack(executionContext.metadata.build()))
            .setResponse(Any.pack(executeResponse))
            .build();

    executionContext.poller.pause();

    try {
      if (!workerContext.putOperation(completedOperation)) {
        return 0;
      }
    } catch (IOException e) {
      log.log(
          Level.SEVERE,
          String.format("error reporting operation complete for %s", operationName),
          e);
      return 0;
    }
    success = true;

    long reportUSecs = stopwatch.elapsed(MICROSECONDS);
    proceedToOutput(completedOperation);
    return stopwatch.elapsed(MICROSECONDS) - reportUSecs;
  }

  private void proceedToOutput(Operation completedExecution) throws InterruptedException {
    ExecutionContext completedExecutionContext =
        executionContext.toBuilder().setOperation(completedExecution).build();
    boolean claimed = owner.output().claim(completedExecutionContext);
    if (claimed) {
      try {
        owner.output().put(completedExecutionContext);
      } catch (InterruptedException e) {
        owner.output().release();
        throw e;
      }
    } else {
      String executionName = executionContext.queueEntry.getExecuteEntry().getOperationName();
      log.log(
          Level.FINER, "ResultReporter: Execution " + executionName + " Failed to claim output");

      owner.error().put(executionContext);
    }
  }

  protected void after(ExecutionContext executionContext) {
    try {
      workerContext.destroyExecDir(executionContext.execDir);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (IOException e) {
      log.log(
          Level.SEVERE,
          String.format("error destroying exec dir %s", executionContext.execDir.toString()),
          e);
    }
  }
}
