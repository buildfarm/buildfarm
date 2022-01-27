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
import static build.buildfarm.common.grpc.TracingMetadataUtils.attachMetadataInterceptor;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.catching;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static java.lang.String.format;

import build.bazel.remote.execution.v2.ActionCacheGrpc;
import build.bazel.remote.execution.v2.ActionCacheGrpc.ActionCacheBlockingStub;
import build.bazel.remote.execution.v2.ActionCacheGrpc.ActionCacheFutureStub;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.BatchReadBlobsRequest;
import build.bazel.remote.execution.v2.BatchReadBlobsResponse;
import build.bazel.remote.execution.v2.BatchReadBlobsResponse.Response;
import build.bazel.remote.execution.v2.BatchUpdateBlobsRequest;
import build.bazel.remote.execution.v2.BatchUpdateBlobsRequest.Request;
import build.bazel.remote.execution.v2.BatchUpdateBlobsResponse;
import build.bazel.remote.execution.v2.CapabilitiesGrpc;
import build.bazel.remote.execution.v2.CapabilitiesGrpc.CapabilitiesBlockingStub;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc.ContentAddressableStorageBlockingStub;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc.ContentAddressableStorageFutureStub;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.ExecutionGrpc;
import build.bazel.remote.execution.v2.ExecutionGrpc.ExecutionStub;
import build.bazel.remote.execution.v2.ExecutionPolicy;
import build.bazel.remote.execution.v2.ExecutionStage;
import build.bazel.remote.execution.v2.FindMissingBlobsRequest;
import build.bazel.remote.execution.v2.FindMissingBlobsResponse;
import build.bazel.remote.execution.v2.GetActionResultRequest;
import build.bazel.remote.execution.v2.GetCapabilitiesRequest;
import build.bazel.remote.execution.v2.GetTreeRequest;
import build.bazel.remote.execution.v2.GetTreeResponse;
import build.bazel.remote.execution.v2.Platform;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.bazel.remote.execution.v2.ResultsCachePolicy;
import build.bazel.remote.execution.v2.ServerCapabilities;
import build.bazel.remote.execution.v2.UpdateActionResultRequest;
import build.bazel.remote.execution.v2.WaitExecutionRequest;
import build.buildfarm.common.CasIndexResults;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.EntryLimitException;
import build.buildfarm.common.Size;
import build.buildfarm.common.Time;
import build.buildfarm.common.Watcher;
import build.buildfarm.common.Write;
import build.buildfarm.common.grpc.ByteStreamHelper;
import build.buildfarm.common.grpc.Retrier;
import build.buildfarm.common.grpc.StubWriteOutputStream;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.MatchListener;
import build.buildfarm.v1test.AdminGrpc;
import build.buildfarm.v1test.AdminGrpc.AdminBlockingStub;
import build.buildfarm.v1test.BackplaneStatus;
import build.buildfarm.v1test.BackplaneStatusRequest;
import build.buildfarm.v1test.GetClientStartTimeRequest;
import build.buildfarm.v1test.GetClientStartTimeResult;
import build.buildfarm.v1test.OperationQueueGrpc;
import build.buildfarm.v1test.OperationQueueGrpc.OperationQueueBlockingStub;
import build.buildfarm.v1test.PollOperationRequest;
import build.buildfarm.v1test.PrepareWorkerForGracefulShutDownRequest;
import build.buildfarm.v1test.PrepareWorkerForGracefulShutDownRequestResults;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.ReindexAllCasRequest;
import build.buildfarm.v1test.ReindexCasRequest;
import build.buildfarm.v1test.ReindexCasRequestResults;
import build.buildfarm.v1test.ShutDownWorkerGracefullyRequest;
import build.buildfarm.v1test.ShutDownWorkerGrpc;
import build.buildfarm.v1test.ShutDownWorkerGrpc.ShutDownWorkerBlockingStub;
import build.buildfarm.v1test.TakeOperationRequest;
import build.buildfarm.v1test.Tree;
import build.buildfarm.v1test.WorkerListMessage;
import build.buildfarm.v1test.WorkerListRequest;
import build.buildfarm.v1test.WorkerProfileGrpc;
import build.buildfarm.v1test.WorkerProfileGrpc.WorkerProfileBlockingStub;
import build.buildfarm.v1test.WorkerProfileMessage;
import build.buildfarm.v1test.WorkerProfileRequest;
import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamGrpc.ByteStreamBlockingStub;
import com.google.bytestream.ByteStreamGrpc.ByteStreamStub;
import com.google.bytestream.ByteStreamProto.ReadRequest;
import com.google.bytestream.ByteStreamProto.ReadResponse;
import com.google.common.base.Functions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.hash.HashCode;
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
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;
import com.google.rpc.Code;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

public class StubInstance implements Instance {
  private static final Logger logger = Logger.getLogger(StubInstance.class.getName());

  private static final long DEFAULT_DEADLINE_DAYS = 100 * 365;

  private final String name;
  private final String identifier;
  private final DigestUtil digestUtil;
  private final ManagedChannel channel;
  private final @Nullable Duration grpcTimeout;
  private final Retrier retrier;
  private final @Nullable ListeningScheduledExecutorService retryService;
  private boolean isStopped = false;
  private final long maxBatchUpdateBlobsSize = Size.mbToBytes(3);

  public StubInstance(String name, DigestUtil digestUtil, ManagedChannel channel) {
    this(name, "no-identifier", digestUtil, channel, Durations.fromDays(DEFAULT_DEADLINE_DAYS));
  }

  public StubInstance(
      String name, String identifier, DigestUtil digestUtil, ManagedChannel channel) {
    this(name, identifier, digestUtil, channel, Durations.fromDays(DEFAULT_DEADLINE_DAYS));
  }

  public StubInstance(
      String name,
      String identifier,
      DigestUtil digestUtil,
      ManagedChannel channel,
      Duration grpcTimeout) {
    this(name, identifier, digestUtil, channel, grpcTimeout, NO_RETRIES, /* retryService=*/ null);
  }

  @SuppressWarnings("NullableProblems")
  public StubInstance(
      String name,
      String identifier,
      DigestUtil digestUtil,
      ManagedChannel channel,
      Duration grpcTimeout,
      Retrier retrier,
      @Nullable ListeningScheduledExecutorService retryService) {
    this.name = name;
    this.identifier = identifier;
    this.digestUtil = digestUtil;
    this.channel = channel;
    this.grpcTimeout = grpcTimeout;
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

  @SuppressWarnings("Guava")
  private final Supplier<ActionCacheBlockingStub> actionCacheBlockingStub =
      Suppliers.memoize(
          new Supplier<ActionCacheBlockingStub>() {
            @Override
            public ActionCacheBlockingStub get() {
              return ActionCacheGrpc.newBlockingStub(channel);
            }
          });

  @SuppressWarnings("Guava")
  private final Supplier<ActionCacheFutureStub> actionCacheFutureStub =
      Suppliers.memoize(
          new Supplier<ActionCacheFutureStub>() {
            @Override
            public ActionCacheFutureStub get() {
              return ActionCacheGrpc.newFutureStub(channel);
            }
          });

  @SuppressWarnings("Guava")
  private final Supplier<CapabilitiesBlockingStub> capsBlockingStub =
      Suppliers.memoize(
          new Supplier<CapabilitiesBlockingStub>() {
            @Override
            public CapabilitiesBlockingStub get() {
              return CapabilitiesGrpc.newBlockingStub(channel);
            }
          });

  @SuppressWarnings("Guava")
  private final Supplier<AdminBlockingStub> adminBlockingStub =
      Suppliers.memoize(
          new Supplier<AdminBlockingStub>() {
            @Override
            public AdminBlockingStub get() {
              return AdminGrpc.newBlockingStub(channel);
            }
          });

  @SuppressWarnings("Guava")
  private final Supplier<ContentAddressableStorageFutureStub> casFutureStub =
      Suppliers.memoize(
          new Supplier<ContentAddressableStorageFutureStub>() {
            @Override
            public ContentAddressableStorageFutureStub get() {
              return ContentAddressableStorageGrpc.newFutureStub(channel);
            }
          });

  @SuppressWarnings("Guava")
  private final Supplier<ContentAddressableStorageBlockingStub> casBlockingStub =
      Suppliers.memoize(
          new Supplier<ContentAddressableStorageBlockingStub>() {
            @Override
            public ContentAddressableStorageBlockingStub get() {
              return ContentAddressableStorageGrpc.newBlockingStub(channel);
            }
          });

  @SuppressWarnings("Guava")
  private final Supplier<ByteStreamStub> bsStub =
      Suppliers.memoize(
          new Supplier<ByteStreamStub>() {
            @Override
            public ByteStreamStub get() {
              return ByteStreamGrpc.newStub(channel);
            }
          });

  @SuppressWarnings("Guava")
  private final Supplier<ByteStreamBlockingStub> bsBlockingStub =
      Suppliers.memoize(
          new Supplier<ByteStreamBlockingStub>() {
            @Override
            public ByteStreamBlockingStub get() {
              return ByteStreamGrpc.newBlockingStub(channel);
            }
          });

  @SuppressWarnings("Guava")
  private final Supplier<OperationsBlockingStub> operationsBlockingStub =
      Suppliers.memoize(
          new Supplier<OperationsBlockingStub>() {
            @Override
            public OperationsBlockingStub get() {
              return OperationsGrpc.newBlockingStub(channel);
            }
          });

  @SuppressWarnings("Guava")
  private final Supplier<OperationQueueBlockingStub> operationQueueBlockingStub =
      Suppliers.memoize(
          new Supplier<OperationQueueBlockingStub>() {
            @Override
            public OperationQueueBlockingStub get() {
              return OperationQueueGrpc.newBlockingStub(channel);
            }
          });

  @SuppressWarnings("Guava")
  private final Supplier<WorkerProfileBlockingStub> workerProfileBlockingStub =
      Suppliers.memoize(
          new Supplier<WorkerProfileBlockingStub>() {
            @Override
            public WorkerProfileBlockingStub get() {
              return WorkerProfileGrpc.newBlockingStub(channel);
            }
          });

  @SuppressWarnings("Guava")
  private final Supplier<ShutDownWorkerBlockingStub> shutDownWorkerBlockingStub =
      Suppliers.memoize(
          new Supplier<ShutDownWorkerBlockingStub>() {
            @Override
            public ShutDownWorkerBlockingStub get() {
              return ShutDownWorkerGrpc.newBlockingStub(channel);
            }
          });

  @SuppressWarnings({"Guava", "ConstantConditions"})
  private <T extends AbstractStub<T>> T deadlined(Supplier<T> getter) {
    T stub = getter.get();
    if (grpcTimeout.getSeconds() > 0 || grpcTimeout.getNanos() > 0) {
      stub = stub.withDeadline(Time.toDeadline(grpcTimeout));
    }
    return stub;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public DigestUtil getDigestUtil() {
    return digestUtil;
  }

  @Override
  public void start(String publicName) {}

  @Override
  public void stop() throws InterruptedException {
    isStopped = true;
    channel.shutdownNow();
    channel.awaitTermination(0, TimeUnit.SECONDS);
    if (retryService != null && !shutdownAndAwaitTermination(retryService, 10, TimeUnit.SECONDS)) {
      logger.log(Level.SEVERE, format("Could not shut down retry service for %s", identifier));
    }
  }

  private void throwIfStopped() {
    if (isStopped) {
      throw new IllegalStateException("instance has been stopped");
    }
  }

  @Override
  public ListenableFuture<ActionResult> getActionResult(
      ActionKey actionKey, RequestMetadata requestMetadata) {
    throwIfStopped();
    return catching(
        deadlined(actionCacheFutureStub)
            .withInterceptors(attachMetadataInterceptor(requestMetadata))
            .getActionResult(
                GetActionResultRequest.newBuilder()
                    .setInstanceName(getName())
                    .setActionDigest(actionKey.getDigest())
                    .build()),
        StatusRuntimeException.class,
        (e) -> {
          if (e.getStatus().equals(Status.NOT_FOUND)) {
            return null;
          }
          throw e;
        },
        directExecutor());
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Override
  public void putActionResult(ActionKey actionKey, ActionResult actionResult) {
    throwIfStopped();
    // should we be checking the ActionResult return value?
    deadlined(actionCacheBlockingStub)
        .updateActionResult(
            UpdateActionResultRequest.newBuilder()
                .setInstanceName(getName())
                .setActionDigest(actionKey.getDigest())
                .setActionResult(actionResult)
                .build());
  }

  @Override
  public ListenableFuture<Iterable<Digest>> findMissingBlobs(
      Iterable<Digest> digests, RequestMetadata requestMetadata) {
    throwIfStopped();
    FindMissingBlobsRequest request =
        FindMissingBlobsRequest.newBuilder()
            .setInstanceName(getName())
            .addAllBlobDigests(digests)
            .build();
    if (request.getSerializedSize() > Size.mbToBytes(4)) {
      throw new IllegalStateException(
          String.format(
              "FINDMISSINGBLOBS IS TOO LARGE: %d digests are required in one request!",
              request.getBlobDigestsCount()));
    }
    return transform(
        deadlined(casFutureStub)
            .withInterceptors(attachMetadataInterceptor(requestMetadata))
            .findMissingBlobs(request),
        FindMissingBlobsResponse::getMissingBlobDigestsList,
        directExecutor());
  }

  @Override
  public Iterable<Digest> putAllBlobs(Iterable<ByteString> blobs, RequestMetadata requestMetadata) {
    long totalSize = 0;
    ImmutableList.Builder<Request> requests = ImmutableList.builder();
    for (ByteString blob : blobs) {
      checkState(totalSize + blob.size() <= maxBatchUpdateBlobsSize);
      requests.add(Request.newBuilder().setDigest(digestUtil.compute(blob)).setData(blob).build());
      totalSize += blob.size();
    }
    BatchUpdateBlobsRequest batchRequest =
        BatchUpdateBlobsRequest.newBuilder()
            .setInstanceName(getName())
            .addAllRequests(requests.build())
            .build();
    BatchUpdateBlobsResponse batchResponse =
        deadlined(casBlockingStub)
            .withInterceptors(attachMetadataInterceptor(requestMetadata))
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
    return Iterables.transform(
        batchResponse.getResponsesList(), BatchUpdateBlobsResponse.Response::getDigest);
  }

  @Override
  public ListenableFuture<Digest> fetchBlob(
      Iterable<String> uris, Digest expectedDigest, RequestMetadata requestMetadata) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Write getOperationStreamWrite(String name) {
    return getWrite(
        name,
        Functions.identity(),
        StubWriteOutputStream.UNLIMITED_EXPECTED_SIZE,
        /* autoflush=*/ true,
        RequestMetadata.getDefaultInstance());
  }

  @Override
  public InputStream newOperationStreamInput(
      String resourceName, long offset, RequestMetadata requestMetadata) throws IOException {
    return newInput(resourceName, offset, requestMetadata);
  }

  InputStream newInput(String resourceName, long offset, RequestMetadata requestMetadata)
      throws IOException {
    return ByteStreamHelper.newInput(
        resourceName,
        offset,
        () -> deadlined(bsStub).withInterceptors(attachMetadataInterceptor(requestMetadata)),
        retrier::newBackoff,
        retrier::isRetriable,
        retryService);
  }

  @Override
  public String getBlobName(Digest blobDigest) {
    return format("%s/blobs/%s", getName(), DigestUtil.toString(blobDigest));
  }

  static class ReadBlobInterchange implements ClientResponseObserver<ReadRequest, ReadResponse> {
    private final ServerCallStreamObserver<ByteString> blobObserver;

    private ClientCallStreamObserver<ReadRequest> requestStream;
    // Guard against spurious onReady() calls caused by a race between onNext() and
    // onReady(). If the transport toggles isReady() from false to true while onNext()
    // is executing, but before onNext() checks isReady(). request(1) would be called
    // twice - once by onNext() and once by the onReady() scheduled during onNext()'s
    // execution.
    @GuardedBy("this")
    private boolean wasReady = false;
    // We must not attempt to call request(1) on the stub until the call has been started.
    @GuardedBy("this")
    private boolean wasStarted = false;
    // Indicator for request completion, so that callbacks throw or are ignored
    final AtomicBoolean wasCompleted = new AtomicBoolean(false);

    ReadBlobInterchange(ServerCallStreamObserver<ByteString> blobObserver) {
      this.blobObserver = blobObserver;
    }

    @Override
    public void beforeStart(final ClientCallStreamObserver<ReadRequest> requestStream) {
      this.requestStream = requestStream;

      requestStream.disableAutoInboundFlowControl();

      blobObserver.setOnCancelHandler(
          () -> {
            if (!wasCompleted.get()) {
              requestStream.onError(Status.CANCELLED.asException());
            }
          });
      blobObserver.setOnReadyHandler(this::onReady);
    }

    void onReady() {
      if (wasCompleted.get()) {
        throw Status.CANCELLED.withDescription("request was completed").asRuntimeException();
      }
      synchronized (this) {
        if (wasStarted && blobObserver.isReady() && !wasReady) {
          wasReady = true;
          checkNotNull(requestStream).request(1);
        }
      }
    }

    @Override
    public void onNext(ReadResponse response) {
      blobObserver.onNext(response.getData());
      synchronized (this) {
        if (blobObserver.isReady()) {
          checkNotNull(requestStream).request(1);
        } else {
          wasReady = false;
        }
        wasStarted = true;
      }
    }

    @Override
    public void onCompleted() {
      wasCompleted.set(true);
      blobObserver.onCompleted();
    }

    @Override
    public void onError(Throwable t) {
      wasCompleted.set(true);
      blobObserver.onError(t);
    }
  }

  @Override
  public void getBlob(
      Digest blobDigest,
      long offset,
      long limit,
      ServerCallStreamObserver<ByteString> blobObserver,
      RequestMetadata requestMetadata) {
    throwIfStopped();
    bsStub
        .get()
        .withInterceptors(attachMetadataInterceptor(requestMetadata))
        .read(
            ReadRequest.newBuilder()
                .setResourceName(getBlobName(blobDigest))
                .setReadOffset(offset)
                .setReadLimit(limit)
                .build(),
            new ReadBlobInterchange(blobObserver));
  }

  @Override
  public InputStream newBlobInput(
      Digest digest,
      long offset,
      long deadlineAfter,
      TimeUnit deadlineAfterUnits,
      RequestMetadata requestMetadata)
      throws IOException {
    return newInput(getBlobName(digest), offset, requestMetadata);
  }

  @Override
  public ListenableFuture<Iterable<Response>> getAllBlobsFuture(Iterable<Digest> digests) {
    return transform(
        deadlined(casFutureStub)
            .batchReadBlobs(
                BatchReadBlobsRequest.newBuilder()
                    .setInstanceName(getName())
                    .addAllDigests(digests)
                    .build()),
        BatchReadBlobsResponse::getResponsesList,
        directExecutor());
  }

  @Override
  public boolean containsBlob(
      Digest digest, Digest.Builder result, RequestMetadata requestMetadata) {
    result.mergeFrom(digest);
    try {
      return Iterables.isEmpty(findMissingBlobs(ImmutableList.of(digest), requestMetadata).get());
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

  Write getWrite(
      String resourceName,
      Function<Throwable, Throwable> exceptionTranslator,
      long expectedSize,
      boolean autoflush,
      RequestMetadata requestMetadata) {
    return new StubWriteOutputStream(
        () ->
            deadlined(bsBlockingStub).withInterceptors(attachMetadataInterceptor(requestMetadata)),
        Suppliers.memoize(
            () ->
                ByteStreamGrpc.newStub(channel)
                    .withInterceptors(attachMetadataInterceptor(requestMetadata))),
        resourceName,
        exceptionTranslator,
        expectedSize,
        autoflush);
  }

  /**
   * no express synchronization, callers are expected to register listeners prior to initiating
   * writes
   */
  @Override
  public Write getBlobWrite(Digest digest, UUID uuid, RequestMetadata requestMetadata) {
    String resourceName =
        ByteStreamUploader.uploadResourceName(
            getName(), uuid, HashCode.fromString(digest.getHash()), digest.getSizeBytes());
    return getWrite(
        resourceName,
        t -> {
          Status status = Status.fromThrowable(t);
          if (status.getCode() == Status.Code.OUT_OF_RANGE) {
            t = new EntryLimitException(status.getDescription());
          }
          return t;
        },
        digest.getSizeBytes(),
        /* autoflush=*/ false,
        requestMetadata);
  }

  @Override
  public String getTree(Digest rootDigest, int pageSize, String pageToken, Tree.Builder tree) {
    tree.setRootDigest(rootDigest);
    throwIfStopped();
    Iterator<GetTreeResponse> replies =
        deadlined(casBlockingStub)
            .getTree(
                GetTreeRequest.newBuilder()
                    .setInstanceName(getName())
                    .setRootDigest(rootDigest)
                    .setPageSize(pageSize)
                    .setPageToken(pageToken)
                    .build());
    // new streaming interface doesn't really fit with what we're trying to do here...
    String nextPageToken = "";
    while (replies.hasNext()) {
      GetTreeResponse response = replies.next();
      for (Directory directory : response.getDirectoriesList()) {
        tree.putDirectories(digestUtil.compute(directory).getHash(), directory);
      }
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
    TakeOperationRequest request =
        TakeOperationRequest.newBuilder().setInstanceName(getName()).setPlatform(platform).build();
    boolean complete = false;
    while (!complete) {
      listener.onWaitStart();
      try {
        QueueEntry queueEntry;
        try {
          queueEntry = deadlined(operationQueueBlockingStub).take(request);
        } finally {
          listener.onWaitEnd();
        }
        listener.onEntry(queueEntry);
        complete = true;
      } catch (Exception e) {
        Status status = Status.fromThrowable(e);
        if (status.getCode() == Status.Code.CANCELLED && Thread.currentThread().isInterrupted()) {
          InterruptedException intEx = new InterruptedException();
          intEx.addSuppressed(e);
          throw intEx;
        }
        if (status.getCode() != Status.Code.DEADLINE_EXCEEDED) {
          listener.onError(e);
          complete = true;
        }
        // ignore DEADLINE_EXCEEDED to prevent long running request behavior
      }
    }
  }

  @Override
  public BackplaneStatus backplaneStatus() {
    throwIfStopped();
    return deadlined(operationQueueBlockingStub)
        .status(BackplaneStatusRequest.newBuilder().setInstanceName(getName()).build());
  }

  @Override
  public boolean putOperation(Operation operation) {
    throwIfStopped();
    com.google.rpc.Status status = deadlined(operationQueueBlockingStub).put(operation);
    int code = status.getCode();
    if (code != Code.OK.getNumber() && code != Code.INVALID_ARGUMENT.getNumber()) {
      logger.log(
          Level.SEVERE,
          format("putOperation(%s) response was unexpected", operation.getName()),
          StatusProto.toStatusException(status));
    }
    return code == Code.OK.getNumber();
  }

  @Override
  public boolean putAndValidateOperation(Operation operation) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean pollOperation(String operationName, ExecutionStage.Value stage) {
    throwIfStopped();
    com.google.rpc.Status status =
        deadlined(operationQueueBlockingStub)
            .poll(
                PollOperationRequest.newBuilder()
                    .setOperationName(operationName)
                    .setStage(stage)
                    .build());
    int code = status.getCode();
    if (code != Code.OK.getNumber() && code != Code.INVALID_ARGUMENT.getNumber()) {
      logger.log(
          Level.SEVERE,
          format("pollOperation(%s) response was unexpected", operationName),
          StatusProto.toStatusException(status));
    }
    return code == Code.OK.getNumber();
  }

  @Override
  public ListenableFuture<Void> watchOperation(String operationName, Watcher watcher) {
    WaitExecutionRequest request = WaitExecutionRequest.newBuilder().setName(operationName).build();
    SettableFuture<Void> result = SettableFuture.create();
    newExStub()
        .waitExecution(
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
      int pageSize, String pageToken, String filter, ImmutableList.Builder<Operation> operations) {
    throwIfStopped();
    ListOperationsResponse response =
        deadlined(operationsBlockingStub)
            .listOperations(
                ListOperationsRequest.newBuilder()
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
    return deadlined(operationsBlockingStub)
        .getOperation(GetOperationRequest.newBuilder().setName(operationName).build());
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Override
  public void deleteOperation(String operationName) {
    throwIfStopped();
    deadlined(operationsBlockingStub)
        .deleteOperation(DeleteOperationRequest.newBuilder().setName(operationName).build());
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Override
  public void cancelOperation(String operationName) {
    throwIfStopped();
    deadlined(operationsBlockingStub)
        .cancelOperation(CancelOperationRequest.newBuilder().setName(operationName).build());
  }

  @Override
  public ServerCapabilities getCapabilities() {
    throwIfStopped();
    return deadlined(capsBlockingStub)
        .getCapabilities(GetCapabilitiesRequest.newBuilder().setInstanceName(getName()).build());
  }

  @Override
  public WorkerProfileMessage getWorkerProfile() {
    return deadlined(workerProfileBlockingStub)
        .getWorkerProfile(WorkerProfileRequest.newBuilder().build());
  }

  @Override
  public WorkerListMessage getWorkerList() {
    return workerProfileBlockingStub.get().getWorkerList(WorkerListRequest.newBuilder().build());
  }

  @Override
  public GetClientStartTimeResult getClientStartTime(GetClientStartTimeRequest request) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CasIndexResults reindexCas(@Nullable String hostName) {
    throwIfStopped();
    ReindexCasRequestResults proto =
        adminBlockingStub.get().reindexAllCas(ReindexAllCasRequest.newBuilder().build());
    if (hostName != null) {
      proto =
          adminBlockingStub
              .get()
              .reindexCas(ReindexCasRequest.newBuilder().setHostId(hostName).build());
    }
    CasIndexResults results = new CasIndexResults();
    results.removedHosts = proto.getRemovedHosts();
    results.removedKeys = proto.getRemovedKeys();
    results.totalKeys = proto.getTotalKeys();
    return results;
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Override
  public void deregisterWorker(String workerName) {
    throwIfStopped();
    adminBlockingStub
        .get()
        .shutDownWorkerGracefully(
            ShutDownWorkerGracefullyRequest.newBuilder().setWorkerName(workerName).build());
  }

  @Override
  public PrepareWorkerForGracefulShutDownRequestResults shutDownWorkerGracefully() {
    throwIfStopped();
    return shutDownWorkerBlockingStub
        .get()
        .prepareWorkerForGracefulShutdown(
            PrepareWorkerForGracefulShutDownRequest.newBuilder().build());
  }
}
