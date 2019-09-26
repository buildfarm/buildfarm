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
import static io.grpc.Status.INVALID_ARGUMENT;
import static java.lang.String.format;
import static java.util.logging.Level.FINER;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.WARNING;

import build.buildfarm.cas.DigestMismatchException;
import build.buildfarm.common.UrlPath.InvalidResourceNameException;
import build.buildfarm.common.Write;
import build.buildfarm.common.io.FeedbackOutputStream;
import build.buildfarm.instance.Instance;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.protobuf.ByteString;
import io.grpc.Context;
import io.grpc.Context.CancellableContext;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

class WriteStreamObserver implements StreamObserver<WriteRequest> {
  private static final Logger logger = Logger.getLogger(WriteStreamObserver.class.getName());

  private final Instances instances;
  private final long deadlineAfter;
  private final TimeUnit deadlineAfterUnits;
  private final Runnable requestNext;
  private final StreamObserver<WriteResponse> responseObserver;
  private final CancellableContext withCancellation;

  private boolean initialized = false;
  private boolean committed = false;
  private String name = null;
  private Write write = null;
  private Instance instance = null;
  private final AtomicBoolean wasReady = new AtomicBoolean(false);

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
      } catch (RuntimeException e) {
        Status status = Status.fromThrowable(e);
        logger.log(
            status.getCode() == Status.Code.CANCELLED ? FINER : SEVERE,
            "error writing " + (name == null ? request.getResourceName() : name),
            e);
        responseObserver.onError(status.asException());
      }
    }
  }

  void onUncommittedNext(WriteRequest request) {
    if (initialized) {
      handleRequest(request);
    } else {
      initialize(request);
    }
  }

  private Write getWrite(String resourceName)
      throws InstanceNotFoundException, InvalidResourceNameException {
    switch (detectResourceOperation(resourceName)) {
      case UploadBlob:
        return ByteStreamService.getUploadBlobWrite(
            instances.getFromUploadBlob(resourceName),
            parseUploadBlobDigest(resourceName),
            parseUploadBlobUUID(resourceName));
      case OperationStream:
        return ByteStreamService.getOperationStreamWrite(
            instances.getFromOperationStream(resourceName),
            resourceName);
      case Blob:
      default:
        throw INVALID_ARGUMENT
            .withDescription("unknown resource operation for " + resourceName)
            .asRuntimeException();
    }
  }

  void commit() {
    committed = true;
    commitSynchronized();
  }

  synchronized void commitSynchronized() {
    checkNotNull(name);
    checkNotNull(write);

    if (Context.current().isCancelled()) {
      logger.finest(format("skipped delivering committed_size to %s for cancelled context", name));
    } else {
      try {
        commitActive(write.getCommittedSize());
      } catch (RuntimeException e) {
        Status status = Status.fromThrowable(e);
        logger.log(
            status.getCode() == Status.Code.CANCELLED ? FINER : SEVERE,
            "error committing " + name,
            e);
        responseObserver.onError(status.asException());
      }
    }
  }

  void commitActive(long committedSize) {
    WriteResponse response = WriteResponse.newBuilder()
        .setCommittedSize(committedSize)
        .build();

    try {
      logger.finest(format("delivering committed_size for %s of %d", name, committedSize));
      responseObserver.onNext(WriteResponse.newBuilder()
          .setCommittedSize(committedSize)
          .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      logger.log(SEVERE, format("error delivering committed_size to %s", name), e);
    }
  }

  private void initialize(WriteRequest request) {
    String resourceName = request.getResourceName();
    if (resourceName.isEmpty()) {
      responseObserver.onError(INVALID_ARGUMENT
          .withDescription("resource_name is empty")
          .asException());
    } else {
      name = resourceName;
      try {
        write = getWrite(resourceName);
        logger.finest(
            format(
                "registering callback for %s: committed_size = %d, complete = %s",
                resourceName,
                write.getCommittedSize(),
                write.isComplete()));
        write.addListener(
            this::commit,
            withCancellation.fixedContextExecutor(directExecutor()));
        if (!write.isComplete()) {
          initialized = true;
          handleRequest(request);
        }
      } catch (InstanceNotFoundException e) {
        responseObserver.onError(BuildFarmInstances.toStatusException(e));
      } catch (InvalidResourceNameException|RuntimeException e) {
        logger.log(WARNING, format("write: %s", request), e);
        responseObserver.onError(Status.fromThrowable(e).asException());
      }
    }
  }

  private void handleRequest(WriteRequest request) {
    String resourceName = request.getResourceName();
    if (resourceName.isEmpty()) {
      resourceName = name;
    }
    handleWrite(
        resourceName,
        request.getWriteOffset(),
        request.getData(),
        request.getFinishWrite());
  }

  private void handleWrite(
      String resourceName,
      long offset,
      ByteString data,
      boolean finishWrite) {
    long committedSize = write.getCommittedSize();
    if (offset != 0 && offset != committedSize) {
      // we are synchronized here for delivery, but not for asynchronous completion
      // of the write - if it has completed already, and that is the source of the
      // offset mismatch, perform nothing further and release sync to allow the
      // callback to complete the write
      if (!write.isComplete()) {
        responseObserver.onError(INVALID_ARGUMENT
            .withDescription(format("offset %d does not match committed size %d", offset, committedSize))
            .asException());
      }
    } else if (!resourceName.equals(name)) {
      responseObserver.onError(INVALID_ARGUMENT
          .withDescription(format("request resource_name %s does not match previous resource_name %s", resourceName, name))
          .asException());
    } else {
      if (offset == 0 && offset != committedSize) {
        write.reset();
      }

      logger.finest(
          format(
              "writing %d to %s at %d%s", data.size(), name, offset, finishWrite ? " with finish_write" : ""));
      if (!data.isEmpty()) {
        writeData(data);
      }
      if (finishWrite) {
        close();
      }
    }
  }

  private void close() {
    logger.finest("closing stream due to finishWrite for " + name);
    try {
      getOutput().close();
    } catch (DigestMismatchException e) {
      responseObserver.onError(Status.INVALID_ARGUMENT
          .withDescription(e.getMessage())
          .asException());
    } catch (IOException e) {
      logger.log(SEVERE, "error closing stream for " + name, e);
      responseObserver.onError(Status.fromThrowable(e).asException());
    }
  }

  private void writeData(ByteString data) {
    try {
      data.writeTo(getOutput());
      requestNextIfReady();
    } catch (IOException e) {
      if (!committed) {
        logger.log(SEVERE, "error writing data for " + name, e);
        responseObserver.onError(Status.fromThrowable(e).asException());
      }
      // shouldn't we be erroring the stream at this point if !committed?
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
      if (!committed) {
        logger.log(SEVERE, "error getting output stream for " + name, e);
        responseObserver.onError(Status.fromThrowable(e).asException());
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
        logger.log(SEVERE, "error closing output stream after error", e);
      }
    } else {
      if (!withCancellation.isCancelled()) {
        logger.log(
            status.getCode() == Status.Code.CANCELLED ? FINER : SEVERE,
            "cancelling context for " + name,
            t);
        withCancellation.cancel(t);
      }
    }
  }

  @Override
  public void onCompleted() {
    logger.finer("got completed for " + name);
  }
}
