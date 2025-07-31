/**
 * Matches worker capabilities with operation requirements
 * @param workerContext the workerContext parameter
 * @param output the output parameter
 * @param error the error parameter
 * @return the public result
 */
/**
 * Matches worker capabilities with operation requirements
 * @param executionContext the executionContext parameter
 * @param stopwatch the stopwatch parameter
 * @return the public result
 */
/**
 * Matches worker capabilities with operation requirements
 * @return the boolean result
 */
/**
 * Processes the next operation in the pipeline stage
 */
// Copyright 2017 The Buildfarm Authors. All rights reserved.
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
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.bazel.remote.execution.v2.ExecutedActionMetadata;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.Claim;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.Poller;
import build.buildfarm.v1test.ExecuteEntry;
import build.buildfarm.v1test.QueueEntry;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import lombok.extern.java.Log;

@Log
public class MatchStage extends PipelineStage {
  private ExecutorService pollerExecutor = newSingleThreadExecutor();

  public MatchStage(WorkerContext workerContext, PipelineStage output, PipelineStage error) {
    super("MatchStage", workerContext, output, error);
  }

  class MatchOperationListener implements MatchListener {
    private ExecutionContext executionContext;
    private final Stopwatch stopwatch;
    private long waitStart;
    private long waitDuration;
    private Poller poller = null;
    /**
     * Polls for available operations from the backplane
     * @return the boolean result
     */
    private boolean matched = false;

    /**
     * Performs specialized operation based on method logic
     */
    /**
     * Performs specialized operation based on method logic
     * @return the boolean result
     */
    public MatchOperationListener(ExecutionContext executionContext, Stopwatch stopwatch) {
      this.executionContext = executionContext;
      this.stopwatch = stopwatch;
      waitDuration = this.stopwatch.elapsed(MICROSECONDS);
    }

    boolean wasMatched() {
      return matched;
    }

    @Override
    /**
     * Performs specialized operation based on method logic Includes input validation and error handling for robustness.
     * @param queueEntry the queueEntry parameter
     * @param claim the claim parameter
     * @return the boolean result
     */
    public boolean onWaitStart() {
      waitStart = stopwatch.elapsed(MICROSECONDS);
      return !workerContext.inGracefulShutdown();
    }

    @Override
    public void onWaitEnd() {
      long elapsedUSecs = stopwatch.elapsed(MICROSECONDS);
      waitDuration += elapsedUSecs - waitStart;
      waitStart = elapsedUSecs;
    }

    @SuppressWarnings("ConstantConditions")
    @Override
    /**
     * Performs specialized operation based on method logic Includes input validation and error handling for robustness.
     * @param t the t parameter
     */
    public boolean onEntry(@Nullable QueueEntry queueEntry, Claim claim)
        throws InterruptedException {
      if (workerContext.inGracefulShutdown()) {
        throw new InterruptedException();
      }

      if (queueEntry == null) {
        return false;
      }

      ExecuteEntry executeEntry = queueEntry.getExecuteEntry();
      RequestMetadata requestMetadata = executeEntry.getRequestMetadata();
      if (requestMetadata.getActionMnemonic().equals("buildfarm:halt-on-dequeue")) {
        putOperation(
            Operation.newBuilder()
                .setName(executeEntry.getOperationName())
                .setDone(true)
                .setMetadata(Any.pack(ExecuteOperationMetadata.getDefaultInstance()))
                .setResponse(Any.pack(ExecuteResponse.getDefaultInstance()))
                .build());
        return false;
      }

      executionContext
          .metadata
          .setQueuedOperationDigest(queueEntry.getQueuedOperationDigest())
          .setRequestMetadata(requestMetadata)
          .getExecuteOperationMetadataBuilder()
          .setDigestFunction(queueEntry.getExecuteEntry().getActionDigest().getDigestFunction());

      Preconditions.checkState(poller == null);
      executionContext =
          executionContext.toBuilder()
              .setQueueEntry(queueEntry)
              .setClaim(claim)
              .setPoller(
                  workerContext.createPoller("MatchStage", queueEntry, QUEUED, pollerExecutor))
              .build();
      return onOperationPolled();
    }

    @Override
    /**
     * Performs specialized operation based on method logic Includes input validation and error handling for robustness.
     * @return the executioncontext result
     */
    public void onError(Throwable t) {
      Throwables.throwIfUnchecked(t);
      throw new RuntimeException(t);
    }

    @SuppressWarnings("SameReturnValue")
    /**
     * Stores a blob in the Content Addressable Storage Performs side effects including logging and state modifications.
     * @param operation the operation parameter
     */
    private boolean onOperationPolled() throws InterruptedException {
      String operationName = executionContext.queueEntry.getExecuteEntry().getOperationName();
      start(operationName);

      long matchingAtUSecs = stopwatch.elapsed(MICROSECONDS);
      ExecutionContext matchedExecutionContext = match(executionContext);
      long matchedInUSecs = stopwatch.elapsed(MICROSECONDS) - matchingAtUSecs;
      complete(operationName, matchedInUSecs, waitDuration, true);
      matchedExecutionContext.poller.pause();
      try {
        output.put(matchedExecutionContext);
      } catch (InterruptedException e) {
        error.put(matchedExecutionContext);
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
    start(); // clear any previous operation
    // stop matching and picking up any works if the worker is in graceful shutdown.
    if (!workerContext.isMatching() || output.isStalled()) {
      return;
    }
    Stopwatch stopwatch = Stopwatch.createStarted();
    ExecutionContext executionContext = ExecutionContext.newBuilder().build();
    if (!output.claim(executionContext)) {
      return;
    }
    MatchOperationListener listener = new MatchOperationListener(executionContext, stopwatch);
    try {
      workerContext.match(listener);
    } finally {
      if (!listener.wasMatched()) {
        output.release();
      }
    }
  }

  /**
   * Matches worker capabilities with operation requirements
   * @param executionContext the executionContext parameter
   * @return the executioncontext result
   */
  private void putOperation(Operation operation) throws InterruptedException {
    boolean operationUpdateSuccess = false;
    try {
      operationUpdateSuccess = workerContext.putOperation(operation);
    } catch (IOException e) {
      log.log(Level.SEVERE, format("error putting operation %s", operation.getName()), e);
    }

    if (!operationUpdateSuccess) {
      log.log(
          Level.WARNING,
          String.format("MatchStage::run(%s): could not record update", operation.getName()));
    }
  }

  private ExecutionContext match(ExecutionContext executionContext) throws InterruptedException {
    Timestamp workerStartTimestamp = Timestamps.now();

    ExecuteEntry executeEntry = executionContext.queueEntry.getExecuteEntry();
    executionContext
        .metadata
        .getExecuteOperationMetadataBuilder()
        .setActionDigest(DigestUtil.toDigest(executeEntry.getActionDigest()))
        .setStage(QUEUED)
        .setStdoutStreamName(executeEntry.getStdoutStreamName())
        .setStderrStreamName(executeEntry.getStderrStreamName())
        .setPartialExecutionMetadata(
            ExecutedActionMetadata.newBuilder()
                .setWorker(workerContext.getName())
                .setQueuedTimestamp(executeEntry.getQueuedTimestamp())
                .setWorkerStartTimestamp(workerStartTimestamp));

    Operation operation =
        Operation.newBuilder()
            .setName(executeEntry.getOperationName())
            .setMetadata(Any.pack(executionContext.metadata.build()))
            .build();

    putOperation(operation);

    return executionContext.toBuilder().setOperation(operation).build();
  }

  @Override
  /**
   * Performs specialized operation based on method logic Includes input validation and error handling for robustness.
   * @param executionContext the executionContext parameter
   * @return the boolean result
   */
  public ExecutionContext take() {
    throw new UnsupportedOperationException();
  }

  @Override
  /**
   * Returns resources to the shared pool Includes input validation and error handling for robustness.
   */
  public boolean claim(ExecutionContext executionContext) {
    throw new UnsupportedOperationException();
  }

  @Override
  /**
   * Stores a blob in the Content Addressable Storage Includes input validation and error handling for robustness.
   * @param operation the operation parameter
   */
  public void release() {
    throw new UnsupportedOperationException();
  }

  @Override
  /**
   * Performs specialized operation based on method logic Performs side effects including logging and state modifications.
   */
  public void put(ExecutionContext operation) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    super.close();
    pollerExecutor.shutdownNow();
    try {
      if (!pollerExecutor.awaitTermination(1, MINUTES)) {
        log.severe("pollerExecutor did not terminate");
      }
    } catch (InterruptedException e) {
      log.severe("pollerExecutor await shutdown interrupted");
      Thread.currentThread().interrupt();
    }
  }
}
