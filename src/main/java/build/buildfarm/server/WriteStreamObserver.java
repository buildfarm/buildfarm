// Copyright 2019 The Bazel Authors. All rights reserved.
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

package build.buildfarm.server;

import static build.buildfarm.common.UrlPath.detectResourceOperation;
import static build.buildfarm.common.UrlPath.parseUploadBlobDigest;
import static build.buildfarm.common.UrlPath.parseUploadBlobUUID;
import static build.buildfarm.common.grpc.Retrier.DEFAULT_IS_RETRIABLE;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.grpc.Status.ABORTED;
import static io.grpc.Status.INVALID_ARGUMENT;
import static java.lang.String.format;

import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.cas.DigestMismatchException;
import build.buildfarm.common.EntryLimitException;
import build.buildfarm.common.UrlPath.InvalidResourceNameException;
import build.buildfarm.common.Write;
import build.buildfarm.common.grpc.TracingMetadataUtils;
import build.buildfarm.common.io.FeedbackOutputStream;
import build.buildfarm.instance.Instance;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.protobuf.ByteString;
import io.grpc.Context;
import io.grpc.Context.CancellableContext;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.concurrent.GuardedBy;

class WriteStreamObserver implements StreamObserver<WriteRequest> {
  private static final Logger logger = Logger.getLogger(WriteStreamObserver.class.getName());

  private final Instances instances;
  private final long deadlineAfter;
  private final TimeUnit deadlineAfterUnits;
  private final Runnable requestNext;
  private final StreamObserver<WriteResponse> responseObserver;
  private final CancellableContext withCancellation;

  private boolean initialized = false;
  private volatile boolean committed = false;
  private String name = null;
  private Write write = null;
  private Instance instance = null;
  private final AtomicReference<Throwable> exception = new AtomicReference<>(null);
  private final AtomicBoolean wasReady = new AtomicBoolean(false);
  private long expectedCommittedSize = -1;
  private long earliestOffset = -1;
  private long requestCount = 0;
  private long requestBytes = 0;

  WriteStreamObserver(
      Instances instances,
      long deadlineAfter,
      TimeUnit deadlineAfterUnits,
      Runnable requestNext,
      StreamObserver<WriteResponse> responseObserver) {
    this.instances = instances;
    this.deadlineAfter = deadlineAfter;
    this.deadlineAfterUnits = deadlineAfterUnits;
    this.requestNext = requestNext;
    this.responseObserver = responseObserver;
    withCancellation = Context.current().withCancellation();
  }

  @Override
  public synchronized void onNext(WriteRequest request) {
    if (!committed) {
      try {
        onUncommittedNext(request);
      } catch (EntryLimitException e) {
        errorResponse(e);
      } catch (Exception e) {
        Status status = Status.fromThrowable(e);
        if (errorResponse(status.asException())) {
          logger.log(
              status.getCode() == Status.Code.CANCELLED ? Level.FINE : Level.SEVERE,
              format("error writing %s", (name == null ? request.getResourceName() : name)),
              e);
        }
      }
    }
  }

  @GuardedBy("this")
  void onUncommittedNext(WriteRequest request) throws EntryLimitException {
    if (initialized) {
      handleRequest(request);
    } else {
      initialize(request);
    }
  }

  private Write getWrite(String resourceName)
      throws EntryLimitException, InstanceNotFoundException, InvalidResourceNameException {
    switch (detectResourceOperation(resourceName)) {
      case UploadBlob:
        Digest uploadBlobDigest = parseUploadBlobDigest(resourceName);
        expectedCommittedSize = uploadBlobDigest.getSizeBytes();
        return ByteStreamService.getUploadBlobWrite(
            instances.getFromUploadBlob(resourceName),
            uploadBlobDigest,
            parseUploadBlobUUID(resourceName));
      case OperationStream:
        return ByteStreamService.getOperationStreamWrite(
            instances.getFromOperationStream(resourceName), resourceName);
      case Blob:
      default:
        throw INVALID_ARGUMENT
            .withDescription("unknown resource operation for " + resourceName)
            .asRuntimeException();
    }
  }

  void commit(long committedSize) {
    committed = true;
    commitSynchronized(committedSize);
  }

  synchronized void commitSynchronized(long committedSize) {
    checkNotNull(name);
    checkNotNull(write);

    if (Context.current().isCancelled()) {
      logger.log(
          Level.FINER,
          format("skipped delivering committed_size to %s for cancelled context", name));
    } else {
      try {
        if (expectedCommittedSize >= 0 && expectedCommittedSize != committedSize) {
          logger.log(
              Level.WARNING,
              format(
                  "committed size %d did not match expectation for %s "
                      + " after %d requests and %d bytes at offset %d, ignoring it",
                  committedSize, name, requestCount, requestBytes, earliestOffset));
          committedSize = expectedCommittedSize;
        }
        commitActive(committedSize);
      } catch (RuntimeException e) {
        RequestMetadata requestMetadata = TracingMetadataUtils.fromCurrentContext();
        Status status = Status.fromThrowable(e);
        if (errorResponse(status.asException())) {
          logger.log(
              status.getCode() == Status.Code.CANCELLED ? Level.FINE : Level.SEVERE,
              format(
                  "%s-%s: %s -> %s -> %s: error committing %s",
                  requestMetadata.getToolDetails().getToolName(),
                  requestMetadata.getToolDetails().getToolVersion(),
                  requestMetadata.getCorrelatedInvocationsId(),
                  requestMetadata.getToolInvocationId(),
                  requestMetadata.getActionId(),
                  name),
              e);
        }
      }
    }
  }

  void commitActive(long committedSize) {
    WriteResponse response = WriteResponse.newBuilder().setCommittedSize(committedSize).build();

    if (exception.compareAndSet(null, null)) {
      try {
        logger.log(
            Level.FINER, format("delivering committed_size for %s of %d", name, committedSize));
        responseObserver.onNext(response);
        responseObserver.onCompleted();
      } catch (Exception e) {
        logger.log(Level.SEVERE, format("error delivering committed_size to %s", name), e);
      }
    }
  }

  @GuardedBy("this")
  private void initialize(WriteRequest request) throws EntryLimitException {
    String resourceName = request.getResourceName();
    if (resourceName.isEmpty()) {
      errorResponse(INVALID_ARGUMENT.withDescription("resource_name is empty").asException());
    } else {
      name = resourceName;
      try {
        write = getWrite(resourceName);
        logger.log(
            Level.FINER,
            format(
                "registering callback for %s: committed_size = %d, complete = %s",
                resourceName, write.getCommittedSize(), write.isComplete()));
        Futures.addCallback(
            write.getFuture(),
            new FutureCallback<Long>() {
              @Override
              public void onSuccess(Long committedSize) {
                commit(committedSize);
              }

              @Override
              public void onFailure(Throwable t) {
                errorResponse(t);
              }
            },
            withCancellation.fixedContextExecutor(directExecutor()));
        if (!write.isComplete()) {
          initialized = true;
          handleRequest(request);
        }
      } catch (EntryLimitException e) {
        errorResponse(e);
      } catch (InstanceNotFoundException e) {
        if (errorResponse(BuildFarmInstances.toStatusException(e))) {
          logWriteRequest(Level.WARNING, request, e);
        }
      } catch (Exception e) {
        if (errorResponse(Status.fromThrowable(e).asException())) {
          logWriteRequest(Level.WARNING, request, e);
        }
      }
    }
  }

  private void logWriteRequest(Level level, WriteRequest request, Exception e) {
    logger.log(
        Level.WARNING,
        format(
            "write: %s, %d bytes%s",
            request.getResourceName(),
            request.getData().size(),
            request.getFinishWrite() ? ", finish_write" : ""),
        e);
  }

  private boolean errorResponse(Throwable t) {
    if (exception.compareAndSet(null, t)) {
      boolean isEntryLimitException = t instanceof EntryLimitException;
      if (isEntryLimitException) {
        t = Status.OUT_OF_RANGE.withDescription(t.getMessage()).asException();
      }
      responseObserver.onError(t);
      if (isEntryLimitException) {
        RequestMetadata requestMetadata = TracingMetadataUtils.fromCurrentContext();
        logger.log(
            Level.WARNING,
            format(
                "%s-%s: %s -> %s -> %s: exceeded entry limit for %s",
                requestMetadata.getToolDetails().getToolName(),
                requestMetadata.getToolDetails().getToolVersion(),
                requestMetadata.getCorrelatedInvocationsId(),
                requestMetadata.getToolInvocationId(),
                requestMetadata.getActionId(),
                name));
      }
      return true;
    }
    return false;
  }

  @GuardedBy("this")
  private void handleRequest(WriteRequest request) throws EntryLimitException {
    String resourceName = request.getResourceName();
    if (resourceName.isEmpty()) {
      resourceName = name;
    }
    handleWrite(
        resourceName, request.getWriteOffset(), request.getData(), request.getFinishWrite());
  }

  @GuardedBy("this")
  private void handleWrite(String resourceName, long offset, ByteString data, boolean finishWrite)
      throws EntryLimitException {
    long committedSize = write.getCommittedSize();
    if (offset != 0 && offset != committedSize) {
      // we are synchronized here for delivery, but not for asynchronous completion
      // of the write - if it has completed already, and that is the source of the
      // offset mismatch, perform nothing further and release sync to allow the
      // callback to complete the write
      //
      // ABORTED response is specific to encourage the client to retry
      errorResponse(
          ABORTED
              .withDescription(
                  format("offset %d does not match committed size %d", offset, committedSize))
              .asException());
    } else if (!resourceName.equals(name)) {
      errorResponse(
          INVALID_ARGUMENT
              .withDescription(
                  format(
                      "request resource_name %s does not match previous resource_name %s",
                      resourceName, name))
              .asException());
    } else {
      if (offset == 0 && offset != committedSize) {
        write.reset();
      }
      if (earliestOffset < 0 || offset < earliestOffset) {
        earliestOffset = offset;
      }

      logger.log(
          Level.FINER,
          format(
              "writing %d to %s at %d%s",
              data.size(), name, offset, finishWrite ? " with finish_write" : ""));
      if (!data.isEmpty()) {
        writeData(data);
        requestCount++;
        requestBytes += data.size();
      }
      if (finishWrite) {
        close();
      }
    }
  }

  private void close() {
    logger.log(Level.FINER, format("closing stream due to finishWrite for %s", name));
    try {
      getOutput().close();
    } catch (DigestMismatchException e) {
      errorResponse(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
    } catch (IOException e) {
      if (errorResponse(Status.fromThrowable(e).asException())) {
        logger.log(Level.SEVERE, format("error closing stream for %s", name), e);
      }
    }
  }

  @GuardedBy("this")
  private void writeData(ByteString data) throws EntryLimitException {
    try {
      data.writeTo(getOutput());
      requestNextIfReady();
    } catch (EntryLimitException e) {
      throw e;
    } catch (IOException e) {
      if (errorResponse(Status.fromThrowable(e).asException())) {
        logger.log(Level.SEVERE, format("error writing data for %s", name), e);
      }
    }
  }

  private void onNewlyReadyRequestNext() {
    if (wasReady.compareAndSet(false, true)) {
      requestNext.run();
    }
  }

  private void requestNextIfReady(FeedbackOutputStream out) {
    if (out.isReady()) {
      requestNext.run();
    } else {
      wasReady.set(false);
    }
  }

  private void requestNextIfReady() {
    try {
      requestNextIfReady(getOutput());
    } catch (IOException e) {
      if (errorResponse(Status.fromThrowable(e).asException())) {
        logger.log(Level.SEVERE, format("error getting output stream for %s", name), e);
      }
    }
  }

  private FeedbackOutputStream getOutput() throws IOException {
    return write.getOutput(deadlineAfter, deadlineAfterUnits, this::onNewlyReadyRequestNext);
  }

  @Override
  public void onError(Throwable t) {
    Status status = Status.fromThrowable(t);
    if (initialized && !DEFAULT_IS_RETRIABLE.apply(status)) {
      try {
        getOutput().close();
      } catch (IOException e) {
        logger.log(Level.SEVERE, "error closing output stream after error", e);
      }
    } else {
      if (!withCancellation.isCancelled()) {
        logger.log(
            status.getCode() == Status.Code.CANCELLED ? Level.FINE : Level.SEVERE,
            format("cancelling context for %s", name),
            t);
        withCancellation.cancel(t);
      }
    }
  }

  @Override
  public void onCompleted() {
    logger.log(Level.FINE, format("got completed for %s", name));
  }
}
