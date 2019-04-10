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

import static build.buildfarm.common.grpc.Retrier.NO_RETRIES;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.String.format;
import static java.util.logging.Level.SEVERE;

import build.bazel.remote.execution.v2.ActionCacheGrpc;
import build.bazel.remote.execution.v2.ActionCacheGrpc.ActionCacheBlockingStub;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.BatchReadBlobsRequest;
import build.bazel.remote.execution.v2.BatchReadBlobsResponse.Response;
import build.bazel.remote.execution.v2.BatchUpdateBlobsRequest;
import build.bazel.remote.execution.v2.BatchUpdateBlobsRequest.Request;
import build.bazel.remote.execution.v2.BatchUpdateBlobsResponse;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc.ContentAddressableStorageBlockingStub;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc.ContentAddressableStorageFutureStub;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecutionGrpc;
import build.bazel.remote.execution.v2.ExecutionGrpc.ExecutionStub;
import build.bazel.remote.execution.v2.WaitExecutionRequest;
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
import build.buildfarm.common.Write;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.Watcher;
import build.buildfarm.common.function.InterruptingPredicate;
import build.buildfarm.common.grpc.ByteStreamHelper;
import build.buildfarm.common.grpc.Retrier;
import build.buildfarm.common.grpc.StubWriteOutputStream;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.Utils;
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
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.longrunning.CancelOperationRequest;
import com.google.longrunning.DeleteOperationRequest;
import com.google.longrunning.GetOperationRequest;
import com.google.longrunning.ListOperationsRequest;
import com.google.longrunning.ListOperationsResponse;
import com.google.longrunning.Operation;
import com.google.longrunning.OperationsGrpc;
import com.google.longrunning.OperationsGrpc.OperationsBlockingStub;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import javax.annotation.Nullable;

public class StubInstance implements Instance {
  private static final Logger logger = Logger.getLogger(StubInstance.class.getName());

  private static final long DEFAULT_DEADLINE_DAYS = 100 * 365;

  private final String name;
  private final String identifier;
  private final DigestUtil digestUtil;
  private final ManagedChannel channel;
  private final long deadlineAfter;
  private final TimeUnit deadlineAfterUnits;
  private final Retrier retrier;
  private final @Nullable ListeningScheduledExecutorService retryService;
  private boolean isStopped = false;
  private final int maxBatchUpdateBlobsSize = 3 * 1024 * 1024;

  public StubInstance(
      String name,
      DigestUtil digestUtil,
      ManagedChannel channel) {
    this(name, "no-identifier", digestUtil, channel, DEFAULT_DEADLINE_DAYS, TimeUnit.DAYS);
  }

  public StubInstance(
      String name,
      String identifier,
      DigestUtil digestUtil,
      ManagedChannel channel) {
    this(name, identifier, digestUtil, channel, DEFAULT_DEADLINE_DAYS, TimeUnit.DAYS);
  }

  public StubInstance(
      String name,
      String identifier,
      DigestUtil digestUtil,
      ManagedChannel channel,
      long deadlineAfter, TimeUnit deadlineAfterUnits) {
    this(name, identifier, digestUtil, channel, deadlineAfter, deadlineAfterUnits, NO_RETRIES, /* retryService=*/ null);
  }

  public StubInstance(
      String name,
      String identifier,
      DigestUtil digestUtil,
      ManagedChannel channel,
      long deadlineAfter, TimeUnit deadlineAfterUnits,
      Retrier retrier,
      @Nullable ListeningScheduledExecutorService retryService) {
    this.name = name;
    this.identifier = identifier;
    this.digestUtil = digestUtil;
    this.channel = channel;
    this.deadlineAfter = deadlineAfter;
    this.deadlineAfterUnits = deadlineAfterUnits;
    this.retrier = retrier;
    this.retryService = retryService;
  }

  public Channel getChannel() {
    return channel;
  }

  // no deadline for this
  private ExecutionStub newExStub() {
    return ExecutionGrpc.newStub(channel);
  }

  private final Supplier<ActionCacheBlockingStub> actionCacheBlockingStub =
      Suppliers.memoize(
          new Supplier<ActionCacheBlockingStub>() {
            @Override
            public ActionCacheBlockingStub get() {
              return ActionCacheGrpc.newBlockingStub(channel);
            }
          });

  private final Supplier<ContentAddressableStorageFutureStub> casFutureStub =
      Suppliers.memoize(
          new Supplier<ContentAddressableStorageFutureStub>() {
            @Override
            public ContentAddressableStorageFutureStub get() {
              return ContentAddressableStorageGrpc.newFutureStub(channel);
            }
          });

  private final Supplier<ContentAddressableStorageBlockingStub> casBlockingStub =
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

  private ByteStreamStub newBSStub() {
    return ByteStreamGrpc.newStub(channel)
        .withDeadlineAfter(deadlineAfter, deadlineAfterUnits);
  }

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
  public ListenableFuture<Iterable<Digest>> findMissingBlobs(Iterable<Digest> digests, Executor executor) {
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
        executor);
  }

  public Write getOperationStreamWrite(String name) {
    return getWrite(name, StubWriteOutputStream.UNLIMITED_EXPECTED_SIZE, /* autoflush=*/ true);
  }

  @Override
  public InputStream newOperationStreamInput(String resourceName, long offset) {
    return newInput(resourceName, offset);
  }

  InputStream newInput(String resourceName, long offset) {
    return ByteStreamHelper.newInput(
        resourceName,
        offset,
        this::newBSStub,
        retrier::newBackoff,
        retrier::isRetriable,
        retryService);
  }

  @Override
  public String getBlobName(Digest blobDigest) {
    return format(
        "%s/blobs/%s",
        getName(),
        DigestUtil.toString(blobDigest));
  }

  @Override
  public void getBlob(Digest blobDigest, long offset, long limit, StreamObserver<ByteString> blobObserver) {
    throwIfStopped();
    newBSStub()
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

  public InputStream newBlobInput(Digest digest, long offset) {
    return newInput(getBlobName(digest), offset);
  }

  @Override
  public ListenableFuture<Iterable<Response>> getAllBlobsFuture(
      Iterable<Digest> digests) {
    return transform(
        casFutureStub.get()
            .batchReadBlobs(BatchReadBlobsRequest.newBuilder()
                .setInstanceName(getName())
                .addAllDigests(digests)
                .build()),
        (response) -> response.getResponsesList(),
        directExecutor());
  }

  @Override
  public boolean containsBlob(Digest digest) {
    try {
      return Iterables.isEmpty(findMissingBlobs(ImmutableList.of(digest), directExecutor()).get());
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      throw new UncheckedExecutionException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  Write getWrite(String resourceName, long expectedSize, boolean autoflush) {
    return new StubWriteOutputStream(
        bsBlockingStub,
        this::newBSStub,
        resourceName,
        expectedSize,
        autoflush);
  }

  /**
   * no express synchronization, callers are expected to register listeners
   * prior to initiating writes
   */
  @Override
  public Write getBlobWrite(Digest digest, UUID uuid) {
    String resourceName = ByteStreamUploader.getResourceName(uuid, getName(), digest);
    return getWrite(resourceName, digest.getSizeBytes(), /* autoflush=*/ false);
  }

  @Override
  public Iterable<Digest> putAllBlobs(Iterable<ByteString> blobs) {
    long totalSize = 0;
    ImmutableList.Builder<Request> requests = ImmutableList.builder();
    for (ByteString blob : blobs) {
      checkState(totalSize + blob.size() <= maxBatchUpdateBlobsSize);
      requests.add(Request.newBuilder()
          .setDigest(digestUtil.compute(blob))
          .setData(blob)
          .build());
      totalSize += blob.size();
    }
    BatchUpdateBlobsRequest batchRequest = BatchUpdateBlobsRequest.newBuilder()
        .setInstanceName(getName())
        .addAllRequests(requests.build())
        .build();
    BatchUpdateBlobsResponse batchResponse = casBlockingStub.get()
        .withDeadlineAfter(deadlineAfter, deadlineAfterUnits)
        .batchUpdateBlobs(batchRequest);
    PutAllBlobsException exception = null;
    for (BatchUpdateBlobsResponse.Response response : batchResponse.getResponsesList()) {
      com.google.rpc.Status status = response.getStatus();
      if (Code.forNumber(status.getCode()) != Code.OK) {
        if (exception == null) {
          exception = new PutAllBlobsException();
        }
        exception.addFailedResponse(response);
      }
    }
    if (exception != null) {
      throw exception;
    }
    return Iterables.transform(batchResponse.getResponsesList(), (response) -> response.getDigest());
  }

  @Override
  public String getTree(
      Digest rootDigest,
      int pageSize,
      String pageToken,
      ImmutableList.Builder<Directory> directories) {
    throwIfStopped();
    Iterator<GetTreeResponse> replies = casBlockingStub.get()
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
      Watcher watcher) {
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
      Watcher watcher) {
    WaitExecutionRequest request = WaitExecutionRequest.newBuilder()
        .setName(operationName)
        .build();
    SettableFuture<Void> result = SettableFuture.create();
    newExStub().waitExecution(
        request,
        new StreamObserver<Operation>() {
          @Override
          public void onNext(Operation operation) {
            watcher.observe(operation);
          }

          @Override
          public void onError(Throwable t) {
            result.setException(t);
          }

          @Override
          public void onCompleted() {
            result.set(null);
          }
        });
    return result;
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
