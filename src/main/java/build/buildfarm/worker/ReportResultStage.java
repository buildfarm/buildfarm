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
import static java.util.concurrent.TimeUnit.SECONDS;

import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.bazel.remote.execution.v2.ExecutedActionMetadata;
import build.buildfarm.common.DigestUtil;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import lombok.extern.java.Log;

@Log
public class ReportResultStage extends PipelineStage {
  private final BlockingQueue<ExecutionContext> queue = new ArrayBlockingQueue<>(1);

  public ReportResultStage(WorkerContext workerContext, PipelineStage output, PipelineStage error) {
    super("ReportResultStage", workerContext, output, error);
  }

  @Override
  protected Logger getLogger() {
    return log;
  }

  @Override
  public ExecutionContext take() throws InterruptedException {
    return queue.take();
  }

  @Override
  public void put(ExecutionContext executionContext) throws InterruptedException {
    queue.put(executionContext);
  }

  @Override
  protected ExecutionContext tick(ExecutionContext executionContext) throws InterruptedException {
    workerContext.resumePoller(
        executionContext.poller,
        "ReportResultStage",
        executionContext.queueEntry,
        EXECUTING,
        this::cancelTick,
        Deadline.after(60, SECONDS));
    try {
      return reportPolled(executionContext);
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

  private ExecutionContext reportPolled(ExecutionContext executionContext)
      throws InterruptedException {
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
      return null;
    } catch (IOException e) {
      log.log(Level.SEVERE, String.format("error uploading outputs for %s", operationName), e);
      return null;
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
        return null;
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
        return null;
      }
    } catch (IOException e) {
      log.log(
          Level.SEVERE,
          String.format("error reporting operation complete for %s", operationName),
          e);
      return null;
    }

    return executionContext.toBuilder().setOperation(completedOperation).build();
  }

  @Override
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
