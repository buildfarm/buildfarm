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

import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.grpc.TracingMetadataUtils;
import build.buildfarm.instance.Instance;
import build.buildfarm.v1test.OperationQueueGrpc;
import build.buildfarm.v1test.OperationQueueGrpc.OperationQueueBlockingStub;
import build.buildfarm.v1test.PollOperationRequest;
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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.longrunning.CancelOperationRequest;
import com.google.longrunning.DeleteOperationRequest;
import com.google.longrunning.GetOperationRequest;
import com.google.longrunning.ListOperationsRequest;
import com.google.longrunning.ListOperationsResponse;
import com.google.longrunning.OperationsGrpc;
import com.google.longrunning.OperationsGrpc.OperationsBlockingStub;
import build.bazel.remote.execution.v2.ActionCacheGrpc;
import build.bazel.remote.execution.v2.ActionCacheGrpc.ActionCacheBlockingStub;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc.ContentAddressableStorageBlockingStub;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc.ContentAddressableStorageFutureStub;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecuteRequest;
import build.bazel.remote.execution.v2.ExecutionGrpc;
import build.bazel.remote.execution.v2.ExecutionGrpc.ExecutionFutureStub;
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
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.logging.Logger;

public class StubInstance implements Instance {
  private static final Logger logger = Logger.getLogger(StubInstance.class.getName());

  private final String name;
  private final DigestUtil digestUtil;
  private final ManagedChannel channel;
  private final Retrier retrier;
  private final ByteStreamUploader uploader;
  private final long deadlineAfter;
  private final TimeUnit deadlineAfterUnits;
  private boolean isStopped = false;

  public StubInstance(
      String name,
      DigestUtil digestUtil,
      ManagedChannel channel,
      long deadlineAfter, TimeUnit deadlineAfterUnits,
      Retrier retrier,
      ByteStreamUploader uploader) {
    this.name = name;
    this.digestUtil = digestUtil;
    this.channel = channel;
    this.deadlineAfter = deadlineAfter;
    this.deadlineAfterUnits = deadlineAfterUnits;
    this.retrier = retrier;
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

  private final Supplier<ExecutionFutureStub> executionFutureStub =
      Suppliers.memoize(
          new Supplier<ExecutionFutureStub>() {
            @Override
            public ExecutionFutureStub get() {
              return ExecutionGrpc.newFutureStub(channel);
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
    // we could executor here, but it seems unnecessary
    return Futures.transform(
        contentAddressableStorageFutureStub
            .get()
            .withDeadlineAfter(deadlineAfter, deadlineAfterUnits)
            .findMissingBlobs(request),
        (response) -> response.getMissingBlobDigestsList(),
        service);
  }

  /** expectedSize == -1 for unlimited */
  @Override
  public CommittingOutputStream getStreamOutput(String name, long expectedSize) {
    throwIfStopped();
    return new CommittingOutputStream() {
      SettableFuture<Long> committedFuture = SettableFuture.create();
      boolean closed = false;
      String resourceName = name;
      long writtenBytes = 0;
      StreamObserver<WriteRequest> requestObserver = bsStub.get()
          .withDeadlineAfter(deadlineAfter, deadlineAfterUnits)
          .write(
              new StreamObserver<WriteResponse>() {
                @Override
                public void onNext(WriteResponse reply) {
                  checkState(reply.getCommittedSize() == writtenBytes);
                  requestObserver.onCompleted();
                  committedFuture.set(reply.getCommittedSize());
                }

                @Override
                public void onError(Throwable t) {
                  committedFuture.setException(t);
                }

                @Override
                public void onCompleted() {
                  if (!closed) {
                    logger.severe("Server closed connection before output stream for " + resourceName + " at " + writtenBytes);
                    // FIXME(werkt) better error, status
                    committedFuture.setException(
                        new RuntimeException("Server closed connection before output stream."));
                  }
                }
              }
          );

      @Override
      public void close() {
        if (!closed) {
          closed = true;
          requestObserver.onNext(WriteRequest.newBuilder()
              .setFinishWrite(true)
              .build());
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
        closed = isFinishWrite(writtenBytes + len);
        WriteRequest request = createWriteRequest(ByteString.copyFrom(b, off, len), closed);
        requestObserver.onNext(request);
        writtenBytes += len;
      }

      @Override
      public ListenableFuture<Long> getCommittedFuture() {
        return committedFuture;
      }
    };
  }

  @Override
  public InputStream newStreamInput(String name, long offset) throws IOException, InterruptedException {
    throwIfStopped();
    Iterator<ReadResponse> replies = retrier.execute(() -> bsBlockingStub
        .get()
        .withDeadlineAfter(deadlineAfter, deadlineAfterUnits)
        .read(ReadRequest.newBuilder().setReadOffset(offset).setResourceName(name).build()));
    return new ByteStringIteratorInputStream(Iterators.transform(replies, (reply) -> reply.getData()), retrier);
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
      ImmutableList.Builder<Directory> directories,
      boolean acceptMissing) {
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
  public void execute(
      Digest actionDigest,
      boolean skipCacheLookup,
      ExecutionPolicy executionPolicy,
      ResultsCachePolicy resultsCachePolicy,
      RequestMetadata metadata,
      Predicate<Operation> watcher) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void match(Platform platform, MatchListener listener) throws InterruptedException {
    throwIfStopped();
    TakeOperationRequest request = TakeOperationRequest.newBuilder()
        .setInstanceName(getName())
        .setPlatform(platform)
        .build();
    // not required to call onOperationName
    listener.onWaitStart();
    Operation operation = operationQueueBlockingStub.get()
        .withDeadlineAfter(deadlineAfter, deadlineAfterUnits)
        .take(request);
    listener.onWaitEnd();
    listener.onOperation(operation);
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
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
  public boolean watchOperation(
      String operationName,
      Predicate<Operation> watcher) {
    throw new UnsupportedOperationException();
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
