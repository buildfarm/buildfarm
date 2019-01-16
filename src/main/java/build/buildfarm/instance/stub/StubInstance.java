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

package build.buildfarm.instance.stub;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static java.util.logging.Level.SEVERE;

import build.bazel.remote.execution.v2.ActionCacheGrpc;
import build.bazel.remote.execution.v2.ActionCacheGrpc.ActionCacheBlockingStub;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc.ContentAddressableStorageBlockingStub;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc.ContentAddressableStorageFutureStub;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecutionPolicy;
import build.bazel.remote.execution.v2.FindMissingBlobsRequest;
import build.bazel.remote.execution.v2.FindMissingBlobsResponse;
import build.bazel.remote.execution.v2.GetActionResultRequest;
import build.bazel.remote.execution.v2.GetTreeRequest;
import build.bazel.remote.execution.v2.GetTreeResponse;
import build.bazel.remote.execution.v2.Platform;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.bazel.remote.execution.v2.ResultsCachePolicy;
import build.bazel.remote.execution.v2.ServerCapabilities;
import build.bazel.remote.execution.v2.UpdateActionResultRequest;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.Utils;
import build.buildfarm.instance.Watcher;
import build.buildfarm.v1test.OperationQueueGrpc;
import build.buildfarm.v1test.OperationQueueGrpc.OperationQueueBlockingStub;
import build.buildfarm.v1test.PollOperationRequest;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.TakeOperationRequest;
import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamGrpc.ByteStreamBlockingStub;
import com.google.bytestream.ByteStreamGrpc.ByteStreamStub;
import com.google.bytestream.ByteStreamProto.ReadRequest;
import com.google.bytestream.ByteStreamProto.ReadResponse;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.longrunning.CancelOperationRequest;
import com.google.longrunning.DeleteOperationRequest;
import com.google.longrunning.GetOperationRequest;
import com.google.longrunning.ListOperationsRequest;
import com.google.longrunning.ListOperationsResponse;
import com.google.longrunning.OperationsGrpc;
import com.google.longrunning.OperationsGrpc.OperationsBlockingStub;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
import com.google.watcher.v1.Change;
import com.google.watcher.v1.ChangeBatch;
import com.google.watcher.v1.Request;
import com.google.watcher.v1.WatcherGrpc;
import com.google.watcher.v1.WatcherGrpc.WatcherBlockingStub;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class StubInstance implements Instance {
  private static final Logger logger = Logger.getLogger(StubInstance.class.getName());

  private final String name;
  private final DigestUtil digestUtil;
  private final ManagedChannel channel;
  private final ByteStreamUploader uploader;
  private final long deadlineAfter;
  private final TimeUnit deadlineAfterUnits;
  private boolean isStopped = false;

  public StubInstance(
      String name,
      DigestUtil digestUtil,
      ManagedChannel channel,
      long deadlineAfter, TimeUnit deadlineAfterUnits,
      ByteStreamUploader uploader) {
    this.name = name;
    this.digestUtil = digestUtil;
    this.channel = channel;
    this.deadlineAfter = deadlineAfter;
    this.deadlineAfterUnits = deadlineAfterUnits;
    this.uploader = uploader;
  }

  private final Supplier<ActionCacheBlockingStub> actionCacheBlockingStub =
      Suppliers.memoize(
          new Supplier<ActionCacheBlockingStub>() {
            @Override
            public ActionCacheBlockingStub get() {
              return ActionCacheGrpc.newBlockingStub(channel);
            }
          });

  private final Supplier<ContentAddressableStorageBlockingStub> contentAddressableStorageBlockingStub =
      Suppliers.memoize(
          new Supplier<ContentAddressableStorageBlockingStub>() {
            @Override
            public ContentAddressableStorageBlockingStub get() {
              return ContentAddressableStorageGrpc.newBlockingStub(channel);
            }
          });

  private final Supplier<ContentAddressableStorageFutureStub> contentAddressableStorageFutureStub =
      Suppliers.memoize(
          new Supplier<ContentAddressableStorageFutureStub>() {
            @Override
            public ContentAddressableStorageFutureStub get() {
              return ContentAddressableStorageGrpc.newFutureStub(channel);
            }
          });

  private final Supplier<ByteStreamBlockingStub> bsBlockingStub =
      Suppliers.memoize(
          new Supplier<ByteStreamBlockingStub>() {
            @Override
            public ByteStreamBlockingStub get() {
              return ByteStreamGrpc.newBlockingStub(channel);
            }
          });

  private final Supplier<ByteStreamStub> bsStub =
      Suppliers.memoize(
          new Supplier<ByteStreamStub>() {
            @Override
            public ByteStreamStub get() {
              return ByteStreamGrpc.newStub(channel);
            }
          });

  private final Supplier<OperationsBlockingStub> operationsBlockingStub =
      Suppliers.memoize(
          new Supplier<OperationsBlockingStub>() {
            @Override
            public OperationsBlockingStub get() {
              return OperationsGrpc.newBlockingStub(channel);
            }
          });

  private final Supplier<OperationQueueBlockingStub> operationQueueBlockingStub =
      Suppliers.memoize(
          new Supplier<OperationQueueBlockingStub>() {
            @Override
            public OperationQueueBlockingStub get() {
              return OperationQueueGrpc.newBlockingStub(channel);
            }
          });

  private final Supplier<WatcherBlockingStub> watcherBlockingStub =
      Suppliers.memoize(
          new Supplier<WatcherBlockingStub>() {
            @Override
            public WatcherBlockingStub get() {
              return WatcherGrpc.newBlockingStub(channel);
            }
          });

  @Override
  public String getName() {
    return name;
  }

  @Override
  public DigestUtil getDigestUtil() {
    return digestUtil;
  }

  @Override
  public void start() { }

  @Override
  public void stop() throws InterruptedException {
    isStopped = true;
    channel.shutdownNow();
    channel.awaitTermination(0, TimeUnit.SECONDS);
  }

  private void throwIfStopped() {
    if (isStopped) {
      throw new IllegalStateException("instance has been stopped");
    }
  }

  @Override
  public ActionResult getActionResult(ActionKey actionKey) {
    throwIfStopped();
    try {
      return actionCacheBlockingStub.get()
          .withDeadlineAfter(deadlineAfter, deadlineAfterUnits)
          .getActionResult(GetActionResultRequest.newBuilder()
              .setInstanceName(getName())
              .setActionDigest(actionKey.getDigest())
              .build());
    } catch (StatusRuntimeException e) {
      if (e.getStatus().equals(Status.NOT_FOUND)) {
        return null;
      }
      throw e;
    }
  }

  @Override
  public void putActionResult(ActionKey actionKey, ActionResult actionResult) {
    throwIfStopped();
    // should we be checking the ActionResult return value?
    actionCacheBlockingStub.get()
        .withDeadlineAfter(deadlineAfter, deadlineAfterUnits)
        .updateActionResult(UpdateActionResultRequest.newBuilder()
            .setInstanceName(getName())
            .setActionDigest(actionKey.getDigest())
            .setActionResult(actionResult)
            .build());
  }

  @Override
  public ListenableFuture<Iterable<Digest>> findMissingBlobs(Iterable<Digest> digests, ExecutorService service) {
    throwIfStopped();
    FindMissingBlobsRequest request = FindMissingBlobsRequest.newBuilder()
        .setInstanceName(getName())
        .addAllBlobDigests(digests)
        .build();
    if (request.getSerializedSize() > 4 * 1024 * 1024) {
      throw new IllegalStateException("FINDMISSINGBLOBS IS TOO LARGE");
    }
    return transform(
        contentAddressableStorageFutureStub.get()
            .withDeadlineAfter(deadlineAfter, deadlineAfterUnits)
            .findMissingBlobs(request),
        (response) -> response.getMissingBlobDigestsList(),
        service);
  }

  /** expectedSize == -1 for unlimited */
  @Override
  public CommittingOutputStream getStreamOutput(String name, long expectedSize) {
    throwIfStopped();
    if (expectedSize == 0) {
      return new CommittingOutputStream() {
        @Override
        public void close() {
        }

        void writeLengthFail(int len) throws IOException {
          throw new IOException("write of " + len + " would exceed expectedSize by " + len);
        }

        @Override
        public void write(int b) throws IOException {
          writeLengthFail(1);
        }

        @Override
        public void write(byte[] b) throws IOException {
          writeLengthFail(b.length);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
          writeLengthFail(len);
        }

        @Override
        public ListenableFuture<Long> getCommittedFuture() {
          return immediateFuture(0l);
        }
      };
    }
    return new CommittingOutputStream() {
      boolean closed = false;
      String resourceName = name;
      long writtenBytes = 0;
      SettableFuture<WriteResponse> writeResponseFuture = SettableFuture.create();
      StreamObserver<WriteRequest> requestObserver = bsStub.get()
          .withDeadlineAfter(deadlineAfter, deadlineAfterUnits)
          .write(
              new StreamObserver<WriteResponse>() {
                @Override
                public void onNext(WriteResponse response) {
                  writeResponseFuture.set(response);
                }

                @Override
                public void onError(Throwable t) {
                  writeResponseFuture.setException(
                      new StatusRuntimeException(Status.fromThrowable(t)));
                }

                @Override
                public void onCompleted() {
                  if (!closed && !writeResponseFuture.isDone()) {
                    logger.severe("Server closed connection before output stream for " + resourceName + " at " + writtenBytes);
                    // FIXME(werkt) better error, status
                    writeResponseFuture.setException(
                        new RuntimeException("Server closed connection before output stream."));
                  }
                }
              }
          );

      private long checkWriteResponse() throws IOException {
        try {
          return writeResponseFuture.get().getCommittedSize();
        } catch (ExecutionException e) {
          if (e.getCause() instanceof RuntimeException) {
            throw (RuntimeException) e.getCause();
          }
          if (e.getCause() instanceof IOException) {
            throw (IOException) e.getCause();
          }
          throw new IOException(e.getCause());
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }

      @Override
      public void close() throws IOException {
        boolean finish = !closed && !writeResponseFuture.isDone();
        if (finish) {
          closed = true;
          if (expectedSize < 0 || writtenBytes < expectedSize) {
            WriteRequest.Builder builder = WriteRequest.newBuilder()
                .setFinishWrite(true);
            if (writtenBytes == 0) {
              builder.setResourceName(resourceName);
            }
            requestObserver.onNext(builder.build());
          }
          requestObserver.onCompleted();
        }
        if (checkWriteResponse() != writtenBytes) {
          throw new IOException("committed_size did not match bytes written");
        }
      }

      @Override
      public void write(int b) throws IOException {
        byte[] buf = new byte[1];
        buf[0] = (byte) b;
        write(buf);
      }

      @Override
      public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
      }

      boolean isFinishWrite(long sizeAfterWrite) {
        return expectedSize < 0 ? false : (sizeAfterWrite >= expectedSize);
      }

      WriteRequest createWriteRequest(ByteString chunk, boolean finishWrite) {
        WriteRequest.Builder builder = WriteRequest.newBuilder()
            .setData(chunk)
            .setWriteOffset(writtenBytes)
            .setFinishWrite(finishWrite);
        if (writtenBytes == 0) {
          builder.setResourceName(resourceName);
        }
        return builder.build();
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        if (closed) {
          throw new IOException("stream is closed");
        }
        if (writeResponseFuture.isDone()) {
          long committedSize = checkWriteResponse();
          throw new IOException("write response with committed_size " + committedSize + " received before write");
        }
        if (expectedSize >= 0 && writtenBytes + len > expectedSize) {
          throw new IOException("write of " + len + " would exceed expectedSize by " + (writtenBytes + len - expectedSize));
        }
        WriteRequest request = createWriteRequest(
            ByteString.copyFrom(b, off, len),
            isFinishWrite(writtenBytes + len));
        requestObserver.onNext(request);
        writtenBytes += len;
      }

      @Override
      public ListenableFuture<Long> getCommittedFuture() {
        return transform(writeResponseFuture, (writeResponse) -> writeResponse.getCommittedSize());
      }
    };
  }

  @Override
  public InputStream newStreamInput(String name, long offset) throws IOException {
    throwIfStopped();
    Iterator<ReadResponse> replies = bsBlockingStub
        .get()
        .withDeadlineAfter(deadlineAfter, deadlineAfterUnits)
        .read(ReadRequest.newBuilder().setReadOffset(offset).setResourceName(name).build());
    return new ByteStringIteratorInputStream(Iterators.transform(replies, (reply) -> reply.getData()));
  }

  @Override
  public String getBlobName(Digest blobDigest) {
    return String.format(
        "%s/blobs/%s",
        getName(),
        DigestUtil.toString(blobDigest));
  }

  @Override
  public void getBlob(Digest blobDigest, long offset, long limit, StreamObserver<ByteString> blobObserver) {
    throwIfStopped();
    bsStub.get()
        .withDeadlineAfter(deadlineAfter, deadlineAfterUnits)
        .read(
            ReadRequest.newBuilder()
                .setResourceName(getBlobName(blobDigest))
                .setReadOffset(offset)
                .setReadLimit(limit)
                .build(),
            new StreamObserver<ReadResponse>() {
              @Override
              public void onNext(ReadResponse response) {
                blobObserver.onNext(response.getData());
              }

              @Override
              public void onCompleted() {
                blobObserver.onCompleted();
              }

              @Override
              public void onError(Throwable t) {
                blobObserver.onError(t);
              }
            });
  }

  @Override
  public ChunkObserver getWriteBlobObserver(Digest blobDigest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ChunkObserver getWriteOperationStreamObserver(String operationStream) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getTree(
      Digest rootDigest,
      int pageSize,
      String pageToken,
      ImmutableList.Builder<Directory> directories) {
    throwIfStopped();
    Iterator<GetTreeResponse> replies = contentAddressableStorageBlockingStub
        .get()
        .withDeadlineAfter(deadlineAfter, deadlineAfterUnits)
        .getTree(GetTreeRequest.newBuilder()
            .setInstanceName(getName())
            .setRootDigest(rootDigest)
            .setPageSize(pageSize)
            .setPageToken(pageToken)
            .build());
    // new streaming interface doesn't really fit with what we're trying to do here...
    String nextPageToken = "";
    while (replies.hasNext()) {
      GetTreeResponse response = replies.next();
      directories.addAll(response.getDirectoriesList());
      nextPageToken = response.getNextPageToken();
    }
    return nextPageToken;
  }

  @Override
  public ListenableFuture<Void> execute(
      Digest actionDigest,
      boolean skipCacheLookup,
      ExecutionPolicy executionPolicy,
      ResultsCachePolicy resultsCachePolicy,
      RequestMetadata metadata,
      Consumer<Operation> watcher) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void match(Platform platform, MatchListener listener) throws InterruptedException {
    throwIfStopped();
    TakeOperationRequest request = TakeOperationRequest.newBuilder()
        .setInstanceName(getName())
        .setPlatform(platform)
        .build();
    listener.onWaitStart();
    QueueEntry queueEntry = operationQueueBlockingStub.get()
        .withDeadlineAfter(deadlineAfter, deadlineAfterUnits)
        .take(request);
    listener.onWaitEnd();
    listener.onEntry(queueEntry);
  }

  @Override
  public boolean putOperation(Operation operation) {
    throwIfStopped();
    return operationQueueBlockingStub
        .get()
        .withDeadlineAfter(deadlineAfter, deadlineAfterUnits)
        .put(operation)
        .getCode() == Code.OK.getNumber();
  }

  @Override
  public boolean putAndValidateOperation(Operation operation) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean pollOperation(
      String operationName,
      ExecuteOperationMetadata.Stage stage) {
    throwIfStopped();
    return operationQueueBlockingStub
        .get()
        .withDeadlineAfter(deadlineAfter, deadlineAfterUnits)
        .poll(PollOperationRequest.newBuilder()
            .setOperationName(operationName)
            .setStage(stage)
            .build())
        .getCode() == Code.OK.getNumber();
  }

  @Override
  public ListenableFuture<Void> watchOperation(
      String operationName,
      Consumer<Operation> watcher) {
    Request request = Request.newBuilder()
        .setTarget(operationName)
        .build();
    Iterator<ChangeBatch> replies = watcherBlockingStub.get().watch(request);
    try {
      while (replies.hasNext()) {
        ChangeBatch changeBatch = replies.next();
        for (Change change : changeBatch.getChangesList()) {
          switch (change.getState()) {
            case INITIAL_STATE_SKIPPED:
              break;
            case ERROR:
              try {
                throw StatusProto.toStatusRuntimeException(
                    change.getData().unpack(com.google.rpc.Status.class));
              } catch (InvalidProtocolBufferException e) {
                return immediateFailedFuture(e);
              }
            case DOES_NOT_EXIST:
              return immediateFailedFuture(Status.NOT_FOUND.asRuntimeException());
            case EXISTS:
              Operation o;
              try {
                o = change.getData().unpack(Operation.class);
              } catch (InvalidProtocolBufferException e) {
                return immediateFailedFuture(e);
              }
              watcher.accept(o);
              break;
            default:
              return immediateFailedFuture(new RuntimeException(String.format("Illegal Change State: %s", change.getState())));
          }
        }
      }
    } finally {
      try {
        while (replies.hasNext()) {
          replies.next();
        }
      } catch (StatusRuntimeException e) {
        // ignore
      }
    }
    return immediateFuture(null);
  }

  @Override
  public String listOperations(
      int pageSize, String pageToken, String filter,
      ImmutableList.Builder<Operation> operations) {
    throwIfStopped();
    ListOperationsResponse response =
        operationsBlockingStub.get()
            .withDeadlineAfter(deadlineAfter, deadlineAfterUnits)
            .listOperations(ListOperationsRequest.newBuilder()
            .setName(getName() + "/operations")
            .setPageSize(pageSize)
            .setPageToken(pageToken)
            .setFilter(filter)
            .build());
    operations.addAll(response.getOperationsList());
    return response.getNextPageToken();
  }

  @Override
  public Operation getOperation(String operationName) {
    throwIfStopped();
    return operationsBlockingStub.get()
        .withDeadlineAfter(deadlineAfter, deadlineAfterUnits)
        .getOperation(GetOperationRequest.newBuilder()
            .setName(operationName)
            .build());
  }

  @Override
  public void deleteOperation(String operationName) {
    throwIfStopped();
    operationsBlockingStub.get()
        .withDeadlineAfter(deadlineAfter, deadlineAfterUnits)
        .deleteOperation(DeleteOperationRequest.newBuilder()
            .setName(operationName)
            .build());
  }

  @Override
  public void cancelOperation(String operationName) {
    throwIfStopped();
    operationsBlockingStub.get()
        .withDeadlineAfter(deadlineAfter, deadlineAfterUnits)
        .cancelOperation(CancelOperationRequest.newBuilder()
        .setName(operationName)
        .build());
  }

  @Override
  public ServerCapabilities getCapabilities() {
    throw new UnsupportedOperationException();
  }
}
