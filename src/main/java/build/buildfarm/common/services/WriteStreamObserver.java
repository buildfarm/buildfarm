// Copyright 2019 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.common.services;

import static build.buildfarm.common.resources.UrlPath.detectResourceOperation;
import static build.buildfarm.common.resources.UrlPath.parseUploadBlobCompressor;
import static build.buildfarm.common.resources.UrlPath.parseUploadBlobDigest;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.grpc.Status.ABORTED;
import static io.grpc.Status.CANCELLED;
import static io.grpc.Status.INVALID_ARGUMENT;
import static java.lang.String.format;

import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.cas.DigestMismatchException;
import build.buildfarm.common.EntryLimitException;
import build.buildfarm.common.Write;
import build.buildfarm.common.Write.WriteCompleteException;
import build.buildfarm.common.grpc.TracingMetadataUtils;
import build.buildfarm.common.io.FeedbackOutputStream;
import build.buildfarm.common.resources.UrlPath.InvalidResourceNameException;
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
import io.prometheus.client.Histogram;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import javax.annotation.concurrent.GuardedBy;
import lombok.extern.java.Log;

@Log
public class WriteStreamObserver implements StreamObserver<WriteRequest> {
  private static final Histogram ioMetric =
      Histogram.build()
          .name("io_bytes_write")
          .buckets(new double[] {10, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000})
          .help("Write I/O (bytes)")
          .register();

  private final Instance instance;
  private final long deadlineAfter;
  private final TimeUnit deadlineAfterUnits;
  private final Runnable requestNext;
  private final StreamObserver<WriteResponse> responseObserver;
  private final CancellableContext withCancellation;

  private boolean initialized = false;
  private volatile boolean committed = false;
  private String name = null;

  @GuardedBy("this")
  private Write write = null;

  @GuardedBy("this")
  private FeedbackOutputStream out = null;

  private final AtomicReference<Throwable> exception = new AtomicReference<>(null);
  private final AtomicBoolean wasReady = new AtomicBoolean(false);
  private long expectedCommittedSize = -1;
  private long earliestOffset = -1;
  private long requestCount = 0;
  private long requestBytes = 0;
  private long initialWriteOffset = 0;
  private Compressor.Value compressor;

  public WriteStreamObserver(
      Instance instance,
      long deadlineAfter,
      TimeUnit deadlineAfterUnits,
      Runnable requestNext,
      StreamObserver<WriteResponse> responseObserver) {
    this.instance = instance;
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
          log.log(
              status.getCode() == Status.Code.CANCELLED ? Level.FINER : Level.SEVERE,
              format("error writing %s", (name == null ? request.getResourceName() : name)),
              e);
        }
      }
    }
  }

  @GuardedBy("this")
  void onUncommittedNext(WriteRequest request)
      throws EntryLimitException, InvalidResourceNameException {
    if (initialized) {
      handleRequest(request);
    } else {
      initialize(request);
    }
  }

  private Write getWrite(String resourceName)
      throws EntryLimitException, InvalidResourceNameException {
    switch (detectResourceOperation(resourceName)) {
      case UPLOAD_BLOB_REQUEST:
        expectedCommittedSize = parseUploadBlobDigest(resourceName).getSize();
        return ByteStreamService.getUploadBlobWrite(instance, resourceName);
      case STREAM_OPERATION_REQUEST:
        return ByteStreamService.getOperationStreamWrite(instance, resourceName);
      case DOWNLOAD_BLOB_REQUEST:
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
      log.log(
          Level.FINEST,
          format("skipped delivering committed_size to %s for cancelled context", name));
    } else {
      try {
        if (compressor != Compressor.Value.IDENTITY) {
          // all compressed uploads are expected to be fine with a -1 response
          committedSize = -1;
        } else if (expectedCommittedSize >= 0 && expectedCommittedSize != committedSize) {
          log.log(
              Level.WARNING,
              format(
                  "committed size %d did not match expectation for %s "
                      + " after %d requests and %d bytes at offset %d, ignoring it",
                  committedSize, name, requestCount, requestBytes, earliestOffset));
          committedSize = expectedCommittedSize;
        }
        commitActive(committedSize);
      } catch (RuntimeException e) {
        Status status = Status.fromThrowable(e);
        if (errorResponse(status.asException())) {
          logWriteActivity(
              status.getCode() == Status.Code.CANCELLED ? Level.FINER : Level.SEVERE,
              "committing",
              e);
        }
      }
    }
  }

  void commitActive(long committedSize) {
    WriteResponse response = WriteResponse.newBuilder().setCommittedSize(committedSize).build();

    if (exception.compareAndSet(null, null)) {
      try {
        log.log(
            Level.FINEST, format("delivering committed_size for %s of %d", name, committedSize));
        responseObserver.onNext(response);
        responseObserver.onCompleted();
      } catch (Exception e) {
        log.log(Level.SEVERE, format("error delivering committed_size to %s", name), e);
      }
    }
  }

  @GuardedBy("this")
  private void initialize(WriteRequest request) throws InvalidResourceNameException {
    String resourceName = request.getResourceName();
    if (resourceName.isEmpty()) {
      errorResponse(INVALID_ARGUMENT.withDescription("resource_name is empty").asException());
    } else {
      compressor = parseUploadBlobCompressor(resourceName);
      initialWriteOffset = request.getWriteOffset();
      name = resourceName;
      try {
        write = getWrite(resourceName);
        boolean isReset = request.getWriteOffset() == 0;
        boolean isComplete = write.getFuture().isDone() || expectedCommittedSize == 0;
        if (log.isLoggable(Level.FINEST)) {
          log.log(
              Level.FINEST,
              format(
                  "registering callback for %s: committed_size = %d (transient), complete = %s",
                  resourceName, isReset ? 0 : write.getCommittedSize(), isComplete));
        }
        Futures.addCallback(
            write.getFuture(),
            new FutureCallback<Long>() {
              @Override
              public void onSuccess(Long committedSize) {
                commit(committedSize);
              }

              @SuppressWarnings("NullableProblems")
              @Override
              public void onFailure(Throwable t) {
                if (errorResponse(t)) {
                  logWriteActivity("completing", t);
                }
              }
            },
            withCancellation.fixedContextExecutor(directExecutor()));
        if (!isComplete) {
          initialized = true;
          handleRequest(request);
        }
      } catch (EntryLimitException e) {
        errorResponse(e);
      } catch (Exception e) {
        if (errorResponse(Status.fromThrowable(e).asException())) {
          logWriteRequest(request, e);
        }
      }
    }
  }

  private void logWriteActivity(String activity, Throwable t) {
    logWriteActivity(Level.SEVERE, activity, t);
  }

  private void logWriteActivity(Level level, String activity, Throwable t) {
    RequestMetadata requestMetadata = TracingMetadataUtils.fromCurrentContext();
    log.log(
        level,
        format(
            "%s-%s: %s -> %s -> %s: error %s %s",
            requestMetadata.getToolDetails().getToolName(),
            requestMetadata.getToolDetails().getToolVersion(),
            requestMetadata.getCorrelatedInvocationsId(),
            requestMetadata.getToolInvocationId(),
            requestMetadata.getActionId(),
            activity,
            name),
        t);
  }

  private void logWriteRequest(WriteRequest request, Exception e) {
    log.log(
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
      if (Status.fromThrowable(t).getCode() == Status.Code.CANCELLED
          || Context.current().isCancelled()) {
        return false;
      }
      boolean isEntryLimitException = t instanceof EntryLimitException;
      if (isEntryLimitException) {
        t = Status.OUT_OF_RANGE.withDescription(t.getMessage()).asException();
      }
      responseObserver.onError(t);
      if (isEntryLimitException) {
        RequestMetadata requestMetadata = TracingMetadataUtils.fromCurrentContext();
        log.log(
            Level.WARNING,
            format(
                "%s-%s: %s -> %s -> %s: exceeded entry limit for %s",
                requestMetadata.getToolDetails().getToolName(),
                requestMetadata.getToolDetails().getToolVersion(),
                requestMetadata.getCorrelatedInvocationsId(),
                requestMetadata.getToolInvocationId(),
                requestMetadata.getActionId(),
                name));
      } else {
        log.log(
            Level.WARNING,
            format(
                "error %s after %d requests and %d bytes at offset %d",
                name, requestCount, requestBytes, earliestOffset),
            t);
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
  private long getCommittedSizeForWrite(long offset) throws IOException {
    getOutput(offset); // establish ownership for this output
    // From REAPI:
    //
    //   Note that when writing compressed blobs, the `WriteRequest.write_offset` in
    //   the initial request in a stream refers to the offset in the uncompressed form
    //   of the blob. In subsequent requests, `WriteRequest.write_offset` MUST be the
    //   sum of the first request's 'WriteRequest.write_offset' and the total size of
    //   all the compressed data bundles in the previous requests.
    //   Note that this mixes an uncompressed offset with a compressed byte length,
    //   which is nonsensical, but it is done to fit the semantics of the existing
    //   ByteStream protocol.
    //
    // Here, we assert that for non-initial compressed conditions, we have a stream
    // offset that must be matched based on the initially supplied write offset. In
    // the initial case, we should compare to the reported write committed size, which
    // is always uncompressed, per both the document above and client upload resumption
    if (requestBytes != 0 && compressor != Compressor.Value.IDENTITY) {
      return initialWriteOffset + requestBytes;
    }
    return write.getCommittedSize();
  }

  @GuardedBy("this")
  private void handleWrite(String resourceName, long offset, ByteString data, boolean finishWrite)
      throws EntryLimitException {
    long committedSize;
    try {
      if (offset == 0) {
        write.reset();
        out = null;
      }
      committedSize = getCommittedSizeForWrite(offset);
    } catch (WriteCompleteException e) {
      // write future must be set, ignore this request
      return;
    } catch (IOException e) {
      if (errorResponse(e)) {
        logWriteActivity("querying", e);
      }
      return;
    }
    // might need a particular selection of 'if it has completed already' for compressed here
    if (offset != 0 && offset > committedSize && compressor == Compressor.Value.IDENTITY) {
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
      if (earliestOffset < 0 || offset < earliestOffset) {
        earliestOffset = offset;
      }

      // we may have a committedSize that is larger than our offset, in which case we want
      // to skip the data bytes until the committedSize. This is practical with our streams,
      // since they should be the same content between offset and committedSize
      int bytesToWrite = data.size();
      // relying on other threads to close us out if the compressed stream has been closed early
      if (compressor == Compressor.Value.IDENTITY
          || committedSize != Write.COMPRESSED_EXPECTED_SIZE) {
        if (bytesToWrite == 0 || committedSize - offset >= bytesToWrite) {
          requestNextIfReady();
        } else {
          // committed size is nonsense for compressed streams. Uncompressed + Compressed in
          // sequence
          if (compressor == Compressor.Value.IDENTITY) {
            // constrained to be within bytesToWrite
            bytesToWrite -= (int) (committedSize - offset);
          }
          int skipBytes = data.size() - bytesToWrite;
          if (skipBytes != 0) {
            data = data.substring(skipBytes);
          }
          log.log(
              Level.FINEST,
              format(
                  "writing %d to %s at %d%s",
                  bytesToWrite, name, offset, finishWrite ? " with finish_write" : ""));
          writeData(offset, data);
          requestCount++;
          requestBytes += data.size();
        }
      }
      if (finishWrite) {
        close();
      }
    }
  }

  @GuardedBy("this")
  private void close() {
    log.log(Level.FINEST, format("closing stream due to finishWrite for %s", name));
    try {
      getOutput(Math.max(earliestOffset, 0l)).close();
    } catch (DigestMismatchException e) {
      errorResponse(
          Status.INVALID_ARGUMENT.withDescription(e.getMessage()).withCause(e).asException());
    } catch (WriteCompleteException e) {
      // ignore, write will be closed with future callback
    } catch (IOException e) {
      if (errorResponse(Status.fromThrowable(e).asException())) {
        log.log(Level.SEVERE, format("error closing stream for %s", name), e);
      }
    }
    out = null;
  }

  @GuardedBy("this")
  private void writeData(long offset, ByteString data) throws EntryLimitException {
    try {
      data.writeTo(getOutput(offset));
      requestNextIfReady();
      ioMetric.observe(data.size());
    } catch (EntryLimitException e) {
      throw e;
    } catch (WriteCompleteException e) {
      // ignore, write will be closed with future callback
    } catch (IOException e) {
      if (errorResponse(Status.fromThrowable(e).asException())) {
        log.log(Level.SEVERE, format("error writing data for %s", name), e);
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

  @GuardedBy("this")
  private void requestNextIfReady() {
    try {
      requestNextIfReady(getOutput(earliestOffset));
    } catch (WriteCompleteException e) {
      // ignore, write will be closed with future callback
    } catch (IOException e) {
      if (errorResponse(Status.fromThrowable(e).asException())) {
        log.log(Level.SEVERE, format("error getting output stream for %s", name), e);
      }
    }
  }

  @GuardedBy("this")
  private FeedbackOutputStream getOutput(long offset) throws IOException {
    if (out == null) {
      out =
          write.getOutput(offset, deadlineAfter, deadlineAfterUnits, this::onNewlyReadyRequestNext);
      if (out != null) {
        withCancellation.addListener(
            context -> {
              synchronized (this) {
                if (out != null) {
                  try {
                    out.close();
                  } catch (IOException e) {
                    log.log(Level.SEVERE, format("error closing on cancellation for %s", name), e);
                  }
                  out = null;
                }
              }
            },
            directExecutor());
        // we were already cancelled in this case
        if (out == null) {
          throw CANCELLED.asRuntimeException();
        }
      }
    }
    return out;
  }

  @Override
  public void onError(Throwable t) {
    log.log(Level.FINER, format("write error for %s", name), t);
  }

  @Override
  public synchronized void onCompleted() {
    log.log(Level.FINER, format("write completed for %s", name));
    if (write == null) {
      // we must return with a response lest we emit a grpc warning
      // there can be no meaningful response at this point, as we
      // have no idea what the size was
      responseObserver.onNext(WriteResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }
}
