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
import static build.buildfarm.common.Errors.VIOLATION_TYPE_MISSING;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.logging.Level.SEVERE;

import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.v1test.CompletedOperationMetadata;
import build.buildfarm.v1test.ExecutingOperationMetadata;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.google.rpc.Code;
import com.google.rpc.Status;
import io.grpc.Deadline;
import io.grpc.StatusException;
import io.grpc.protobuf.StatusProto;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

public class ReportResultStage extends PipelineStage {
  private static final Logger logger = Logger.getLogger(ReportResultStage.class.getName());

  private final BlockingQueue<OperationContext> queue = new ArrayBlockingQueue<>(1);

  public ReportResultStage(WorkerContext workerContext, PipelineStage output, PipelineStage error) {
    super("ReportResultStage", workerContext, output, error);
  }

  @Override
  protected Logger getLogger() {
    return logger;
  }

  @Override
  public OperationContext take() throws InterruptedException {
    return queue.take();
  }

  @Override
  public void put(OperationContext operationContext) throws InterruptedException {
    queue.put(operationContext);
  }

  @Override
  protected OperationContext tick(OperationContext operationContext) throws InterruptedException {
    workerContext.resumePoller(
        operationContext.poller,
        "ReportResultStage",
        operationContext.queueEntry,
        EXECUTING,
        this::cancelTick,
        Deadline.after(60, SECONDS));
    try {
      return reportPolled(operationContext);
    } finally {
      operationContext.poller.pause();
    }
  }

  private OperationContext reportPolled(OperationContext operationContext) throws InterruptedException {
    String operationName = operationContext.operation.getName();

    ActionResult.Builder resultBuilder = operationContext.executeResponse.getResultBuilder();
    resultBuilder.getExecutionMetadataBuilder()
        .setOutputUploadStartTimestamp(Timestamps.fromMillis(System.currentTimeMillis()));

    boolean blacklist = false;
    try {
      workerContext.uploadOutputs(
          operationContext.queueEntry.getExecuteEntry().getActionDigest(),
          resultBuilder,
          operationContext.execDir,
          operationContext.command.getOutputFilesList(),
          operationContext.command.getOutputDirectoriesList());
    } catch (StatusException e) {
      if (operationContext.executeResponse.build().getStatus().getCode() == Code.OK.getNumber()) {
        // something about the outputs was malformed - fail the operation with this status if not already failing
        Status status = StatusProto.fromThrowable(e);
        if (status == null) {
          logger.log(SEVERE, "no rpc status from exception for " + operationName, e);
          status = asExecutionStatus(e);
        }
        operationContext.executeResponse.setStatus(status);
        if (isRetriable(status)) {
          blacklist = true;
        }
      }
    } catch (InterruptedException|ClosedByInterruptException e) {
      // cancellation here should not be logged
      return null;
    } catch (IOException e) {
      logger.log(SEVERE, "error while uploading outputs for " + operationName, e);
      return null;
    }

    ExecuteOperationMetadata metadata;
    try {
      metadata = operationContext.operation.getMetadata()
          .unpack(ExecutingOperationMetadata.class)
          .getExecuteOperationMetadata();
    } catch (InvalidProtocolBufferException e) {
      logger.log(SEVERE, "invalid execute operation metadata for " + operationName, e);
      return null;
    }

    Timestamp now = Timestamps.fromMillis(System.currentTimeMillis());
    resultBuilder.getExecutionMetadataBuilder()
        .setWorkerCompletedTimestamp(now)
        .setOutputUploadCompletedTimestamp(now);

    ExecuteResponse executeResponse = operationContext.executeResponse.build();

    if (blacklist || (!operationContext.action.getDoNotCache()
          && executeResponse.getStatus().getCode() == Code.OK.getNumber()
          && executeResponse.getResult().getExitCode() == 0)) {
      try {
        if (blacklist) {
          workerContext.blacklistAction(metadata.getActionDigest().getHash());
        } else {
          workerContext.putActionResult(DigestUtil.asActionKey(metadata.getActionDigest()), executeResponse.getResult());
        }
      } catch (IOException e) {
        logger.log(SEVERE, "error reporting action result for " + operationName, e);
        return null;
      }
    }

    CompletedOperationMetadata completedMetadata = CompletedOperationMetadata.newBuilder()
        .setExecuteOperationMetadata(metadata.toBuilder()
            .setStage(COMPLETED)
            .build())
        .setRequestMetadata(operationContext.queueEntry.getExecuteEntry().getRequestMetadata())
        .build();

    Operation operation = operationContext.operation.toBuilder()
        .setDone(true)
        .setMetadata(Any.pack(completedMetadata))
        .setResponse(Any.pack(executeResponse))
        .build();

    operationContext.poller.pause();

    try {
      if (!workerContext.putOperation(operation, operationContext.action)) {
        return null;
      }
    } catch (IOException e) {
      logger.log(SEVERE, "error reporting complete operation for " + operationName, e);
      return null;
    }

    return operationContext.toBuilder()
        .setOperation(operation)
        .build();
  }

  @Override
  protected void after(OperationContext operationContext) {
    try {
      workerContext.destroyExecDir(operationContext.execDir);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (IOException e) {
      logger.log(SEVERE, "error destroying exec dir " + operationContext.execDir.toString(), e);
    }
  }
}
