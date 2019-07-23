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
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.logging.Level.SEVERE;

import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.Poller;
import build.buildfarm.instance.stub.Chunker;
import build.buildfarm.v1test.CompletedOperationMetadata;
import build.buildfarm.v1test.ExecutingOperationMetadata;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.hash.HashCode;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Durations;
import com.google.rpc.Status;
import com.google.rpc.Code;
import io.grpc.Deadline;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
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

  private DigestUtil getDigestUtil() {
    return workerContext.getDigestUtil();
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
    final Operation operation = operationContext.operation;
    final String operationName = operation.getName();
    ExecuteResponse executeResponse;
    try {
      executeResponse = operation
          .getResponse().unpack(ExecuteResponse.class);
    } catch (InvalidProtocolBufferException e) {
      logger.log(SEVERE, "invalid ExecuteResponse for " + operationName, e);
      return null;
    }

    ActionResult.Builder resultBuilder = executeResponse.getResult().toBuilder();

    long reportStartAt = System.nanoTime();

    Status.Builder status = executeResponse.getStatus().toBuilder();
    try {
      workerContext.uploadOutputs(
          resultBuilder,
          operationContext.execDir,
          operationContext.command.getOutputFilesList(),
          operationContext.command.getOutputDirectoriesList());
    } catch (InterruptedException|ClosedByInterruptException e) {
      // cancellation here should not be logged
      return null;
    } catch (IOException e) {
      logger.log(SEVERE, "error while uploading outputs for " + operationName, e);
      return null;
    }

    ExecuteOperationMetadata metadata;
    try {
      metadata = operation
          .getMetadata()
          .unpack(ExecutingOperationMetadata.class)
          .getExecuteOperationMetadata();
    } catch (InvalidProtocolBufferException e) {
      logger.log(SEVERE, "invalid execute operation metadata for " + operationName, e);
      return null;
    }

    ActionResult result = resultBuilder.build();
    if (!operationContext.action.getDoNotCache() && resultBuilder.getExitCode() == 0) {
      try {
        workerContext.putActionResult(DigestUtil.asActionKey(metadata.getActionDigest()), result);
      } catch (IOException e) {
        logger.log(SEVERE, "error reporting action result for " + operationName, e);
        return null;
      }
    }

    Duration reportedIn = Durations.fromNanos(System.nanoTime() - reportStartAt);

    long completedAt = System.currentTimeMillis();

    CompletedOperationMetadata completedMetadata = CompletedOperationMetadata.newBuilder()
        .setCompletedAt(completedAt)
        .setExecutedOn(workerContext.getName())
        .setMatchedIn(operationContext.matchedIn)
        .setFetchedIn(operationContext.fetchedIn)
        .setExecutedIn(operationContext.executedIn)
        .setReportedIn(reportedIn)
        .setExecuteOperationMetadata(metadata.toBuilder()
            .setStage(COMPLETED)
            .build())
        .setRequestMetadata(operationContext.queueEntry.getExecuteEntry().getRequestMetadata())
        .build();

    Operation doneOperation = operation.toBuilder()
        .setDone(true)
        .setMetadata(Any.pack(completedMetadata))
        .setResponse(Any.pack(executeResponse.toBuilder()
            .setResult(result)
            .setStatus(status)
            .build()))
        .build();

    operationContext.poller.pause();

    try {
      if (!workerContext.putOperation(doneOperation, operationContext.action)) {
        return null;
      }
    } catch (IOException e) {
      logger.log(SEVERE, "error reporting complete operation for " + operationName, e);
      return null;
    }

    return operationContext.toBuilder()
        .setOperation(doneOperation)
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
