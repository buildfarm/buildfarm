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

package build.buildfarm.instance.stub;

import static build.buildfarm.common.grpc.Retrier.NO_RETRIES;
import static build.buildfarm.common.grpc.TracingMetadataUtils.attachMetadataInterceptor;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.Futures.catching;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static java.lang.String.format;

import build.bazel.remote.asset.v1.FetchBlobRequest;
import build.bazel.remote.asset.v1.FetchGrpc;
import build.bazel.remote.asset.v1.FetchGrpc.FetchFutureStub;
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
import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc.ContentAddressableStorageBlockingStub;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc.ContentAddressableStorageFutureStub;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.DigestFunction;
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
import build.bazel.remote.execution.v2.RequestMetadata;
import build.bazel.remote.execution.v2.ResultsCachePolicy;
import build.bazel.remote.execution.v2.ServerCapabilities;
import build.bazel.remote.execution.v2.UpdateActionResultRequest;
import build.bazel.remote.execution.v2.WaitExecutionRequest;
import build.buildfarm.common.CasIndexResults;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.EntryLimitException;
import build.buildfarm.common.Size;
import build.buildfarm.common.Time;
import build.buildfarm.common.Watcher;
import build.buildfarm.common.Write;
import build.buildfarm.common.grpc.ByteStreamHelper;
import build.buildfarm.common.grpc.Retrier;
import build.buildfarm.common.grpc.StubWriteOutputStream;
import build.buildfarm.common.resources.BlobInformation;
import build.buildfarm.common.resources.DownloadBlobRequest;
import build.buildfarm.common.resources.ResourceParser;
import build.buildfarm.common.resources.UploadBlobRequest;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.InstanceBase;
import build.buildfarm.v1test.AdminGrpc;
import build.buildfarm.v1test.AdminGrpc.AdminBlockingStub;
import build.buildfarm.v1test.BackplaneStatus;
import build.buildfarm.v1test.BackplaneStatusRequest;
import build.buildfarm.v1test.BatchWorkerProfilesRequest;
import build.buildfarm.v1test.BatchWorkerProfilesResponse;
import build.buildfarm.v1test.GetClientStartTimeRequest;
import build.buildfarm.v1test.GetClientStartTimeResult;
import build.buildfarm.v1test.OperationQueueGrpc;
import build.buildfarm.v1test.OperationQueueGrpc.OperationQueueBlockingStub;
import build.buildfarm.v1test.PipelineChange;
import build.buildfarm.v1test.PollOperationRequest;
import build.buildfarm.v1test.PrepareWorkerForGracefulShutDownRequest;
import build.buildfarm.v1test.PrepareWorkerForGracefulShutDownRequestResults;
import build.buildfarm.v1test.ReindexCasRequest;
import build.buildfarm.v1test.ReindexCasRequestResults;
import build.buildfarm.v1test.ShutDownWorkerGracefullyRequest;
import build.buildfarm.v1test.Tree;
import build.buildfarm.v1test.WorkerControlGrpc;
import build.buildfarm.v1test.WorkerControlGrpc.WorkerControlBlockingStub;
import build.buildfarm.v1test.WorkerPipelineChangeRequest;
import build.buildfarm.v1test.WorkerPipelineChangeResponse;
import build.buildfarm.v1test.WorkerProfileGrpc;
import build.buildfarm.v1test.WorkerProfileGrpc.WorkerProfileFutureStub;
import build.buildfarm.v1test.WorkerProfileMessage;
import build.buildfarm.v1test.WorkerProfileRequest;
import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamGrpc.ByteStreamBlockingStub;
import com.google.bytestream.ByteStreamGrpc.ByteStreamStub;
import com.google.bytestream.ByteStreamProto.ReadRequest;
import com.google.bytestream.ByteStreamProto.ReadResponse;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import lombok.extern.java.Log;

@Log
public class StubInstance extends InstanceBase {
  private static final long DEFAULT_DEADLINE_DAYS = 100 * 365;

  private final String identifier;
  private final ManagedChannel channel;
  private final ManagedChannel writeChannel;
  private final @Nullable Duration grpcTimeout;
  private final Retrier retrier;
  private final @Nullable ListeningScheduledExecutorService retryService;
  private boolean isStopped = false;

  @GuardedBy("this")
  private Runnable onStopped;

  private final long maxBatchUpdateBlobsSize = Size.mbToBytes(3);

  @VisibleForTesting long maxRequestSize = Size.mbToBytes(4);

  public StubInstance(String name, ManagedChannel channel) {
    this(name, "no-identifier", channel, Durations.fromDays(DEFAULT_DEADLINE_DAYS));
  }

  public StubInstance(String name, String identifier, ManagedChannel channel) {
    this(name, identifier, channel, Durations.fromDays(DEFAULT_DEADLINE_DAYS));
  }

  public StubInstance(
      String name, String identifier, ManagedChannel channel, Duration grpcTimeout) {
    this(name, identifier, channel, grpcTimeout, NO_RETRIES, /* retryService= */ null);
  }

  public StubInstance(
      String name,
      String identifier,
      ManagedChannel channel,
      Duration grpcTimeout,
      Retrier retrier,
      @Nullable ListeningScheduledExecutorService retryService) {
    this(name, identifier, channel, channel, grpcTimeout, retrier, retryService);
  }

  @SuppressWarnings("NullableProblems")
  public StubInstance(
      String name,
      String identifier,
      ManagedChannel channel,
      ManagedChannel writeChannel,
      Duration grpcTimeout,
      Retrier retrier,
      @Nullable ListeningScheduledExecutorService retryService) {
    super(name);
    this.identifier = identifier;
    this.channel = channel;
    this.writeChannel = writeChannel;
    this.grpcTimeout = grpcTimeout;
    this.retrier = retrier;
    this.retryService = retryService;
  }

  public Channel getChannel() {
    return channel;
  }

  public void setOnStopped(Runnable onStopped) {
    if (isStopped) {
      onStopped.run();
    } else {
      synchronized (this) {
        this.onStopped = onStopped;
      }
    }
  }

  // no deadline for this
  private ExecutionStub newExStub() {
    return ExecutionGrpc.newStub(channel);
  }

  @SuppressWarnings("Guava")
  private final Supplier<ActionCacheBlockingStub> actionCacheBlockingStub =
      Suppliers.memoize(
          new Supplier<>() {
            @Override
            public ActionCacheBlockingStub get() {
              return ActionCacheGrpc.newBlockingStub(channel);
            }
          });

  @SuppressWarnings("Guava")
  private final Supplier<ActionCacheFutureStub> actionCacheFutureStub =
      Suppliers.memoize(
          new Supplier<>() {
            @Override
            public ActionCacheFutureStub get() {
              return ActionCacheGrpc.newFutureStub(channel);
            }
          });

  @SuppressWarnings("Guava")
  private final Supplier<CapabilitiesBlockingStub> capsBlockingStub =
      Suppliers.memoize(
          new Supplier<>() {
            @Override
            public CapabilitiesBlockingStub get() {
              return CapabilitiesGrpc.newBlockingStub(channel);
            }
          });

  @SuppressWarnings("Guava")
  private final Supplier<AdminBlockingStub> adminBlockingStub =
      Suppliers.memoize(
          new Supplier<>() {
            @Override
            public AdminBlockingStub get() {
              return AdminGrpc.newBlockingStub(channel);
            }
          });

  private final Supplier<FetchFutureStub> fetchFutureStub =
      Suppliers.memoize(
          new Supplier<>() {
            @Override
            public FetchFutureStub get() {
              return FetchGrpc.newFutureStub(channel);
            }
          });

  @SuppressWarnings("Guava")
  private final Supplier<ContentAddressableStorageFutureStub> casFutureStub =
      Suppliers.memoize(
          new Supplier<>() {
            @Override
            public ContentAddressableStorageFutureStub get() {
              return ContentAddressableStorageGrpc.newFutureStub(channel);
            }
          });

  @SuppressWarnings("Guava")
  private final Supplier<ContentAddressableStorageBlockingStub> casBlockingStub =
      Suppliers.memoize(
          new Supplier<>() {
            @Override
            public ContentAddressableStorageBlockingStub get() {
              return ContentAddressableStorageGrpc.newBlockingStub(channel);
            }
          });

  @SuppressWarnings("Guava")
  private final Supplier<ByteStreamStub> bsStub =
      Suppliers.memoize(
          new Supplier<>() {
            @Override
            public ByteStreamStub get() {
              return ByteStreamGrpc.newStub(channel);
            }
          });

  @SuppressWarnings("Guava")
  private final Supplier<ByteStreamBlockingStub> bsBlockingStub =
      Suppliers.memoize(
          new Supplier<>() {
            @Override
            public ByteStreamBlockingStub get() {
              return ByteStreamGrpc.newBlockingStub(channel);
            }
          });

  @SuppressWarnings("Guava")
  private final Supplier<OperationsBlockingStub> operationsBlockingStub =
      Suppliers.memoize(
          new Supplier<>() {
            @Override
            public OperationsBlockingStub get() {
              return OperationsGrpc.newBlockingStub(channel);
            }
          });

  @SuppressWarnings("Guava")
  private final Supplier<OperationQueueBlockingStub> operationQueueBlockingStub =
      Suppliers.memoize(
          new Supplier<>() {
            @Override
            public OperationQueueBlockingStub get() {
              return OperationQueueGrpc.newBlockingStub(channel);
            }
          });

  @SuppressWarnings("Guava")
  private final Supplier<WorkerProfileFutureStub> workerProfileFutureStub =
      Suppliers.memoize(
          new Supplier<>() {
            @Override
            public WorkerProfileFutureStub get() {
              return WorkerProfileGrpc.newFutureStub(channel);
            }
          });

  @SuppressWarnings("Guava")
  private final Supplier<WorkerControlBlockingStub> workerControlBlockingStub =
      Suppliers.memoize(
          new Supplier<>() {
            @Override
            public WorkerControlBlockingStub get() {
              return WorkerControlGrpc.newBlockingStub(channel);
            }
          });

  @SuppressWarnings({"Guava", "ConstantConditions"})
  private <T extends AbstractStub<T>> T deadlined(Supplier<T> getter) {
    T stub = getter.get();
    if (Durations.isPositive(grpcTimeout)) {
      stub = stub.withDeadline(Time.toDeadline(grpcTimeout));
    }
    return stub;
  }

  @Override
  public void start(String publicName) {}

  @Override
  public void stop() throws InterruptedException {
    isStopped = true;
    if (!channel.isShutdown()) {
      channel.shutdownNow();
      channel.awaitTermination(0, TimeUnit.SECONDS);
    }
    if (!writeChannel.isShutdown()) {
      writeChannel.shutdownNow();
      writeChannel.awaitTermination(0, TimeUnit.SECONDS);
    }
    if (retryService != null && !shutdownAndAwaitTermination(retryService, 10, TimeUnit.SECONDS)) {
      log.log(Level.SEVERE, format("Could not shut down retry service for %s", identifier));
    }
    Runnable onStopped = () -> {};
    synchronized (this) {
      if (this.onStopped != null) {
        onStopped = this.onStopped;
        this.onStopped = null;
      }
    }
    onStopped.run();
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
    build.buildfarm.v1test.Digest actionDigest = actionKey.getDigest();
    return catching(
        deadlined(actionCacheFutureStub)
            .withInterceptors(attachMetadataInterceptor(requestMetadata))
            .getActionResult(
                GetActionResultRequest.newBuilder()
                    .setInstanceName(getName())
                    .setActionDigest(DigestUtil.toDigest(actionDigest))
                    .setDigestFunction(actionDigest.getDigestFunction())
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
    build.buildfarm.v1test.Digest actionDigest = actionKey.getDigest();
    deadlined(actionCacheBlockingStub)
        .updateActionResult(
            UpdateActionResultRequest.newBuilder()
                .setInstanceName(getName())
                .setActionDigest(DigestUtil.toDigest(actionDigest))
                .setDigestFunction(actionDigest.getDigestFunction())
                .setActionResult(actionResult)
                .build());
  }

  @Override
  public ListenableFuture<Iterable<Digest>> findMissingBlobs(
      Iterable<Digest> digests,
      DigestFunction.Value digestFunction,
      RequestMetadata requestMetadata) {
    throwIfStopped();
    FindMissingBlobsRequest request =
        FindMissingBlobsRequest.newBuilder()
            .setInstanceName(getName())
            .addAllBlobDigests(digests)
            .setDigestFunction(digestFunction)
            .build();
    if (request.getSerializedSize() > maxRequestSize) {
      // log2n partition for size reduction as needed
      int partitionSize = (request.getBlobDigestsCount() + 1) / 2;
      return transform(
          allAsList(
              Iterables.transform(
                  Iterables.partition(digests, partitionSize),
                  subDigests -> findMissingBlobs(subDigests, digestFunction, requestMetadata))),
          Iterables::concat,
          directExecutor());
    }
    return transform(
        deadlined(casFutureStub)
            .withInterceptors(attachMetadataInterceptor(requestMetadata))
            .findMissingBlobs(request),
        FindMissingBlobsResponse::getMissingBlobDigestsList,
        directExecutor());
  }

  @Override
  public Iterable<Digest> putAllBlobs(
      Iterable<Request> requests,
      DigestFunction.Value digestFunction,
      RequestMetadata requestMetadata) {
    long totalSize = 0;
    for (Request request : requests) {
      totalSize += request.getData().size();
      checkState(totalSize <= maxBatchUpdateBlobsSize);
    }
    BatchUpdateBlobsRequest batchRequest =
        BatchUpdateBlobsRequest.newBuilder()
            .setInstanceName(getName())
            .addAllRequests(requests)
            .setDigestFunction(digestFunction)
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
  public ListenableFuture<build.buildfarm.v1test.Digest> fetchBlob(
      Iterable<String> uris,
      Map<String, String> headers,
      build.buildfarm.v1test.Digest expectedDigest,
      RequestMetadata requestMetadata) {
    // needs to add expectedDigest, digestFunction, etc
    FetchBlobRequest request =
        FetchBlobRequest.newBuilder()
            .addAllUris(uris)
            .setDigestFunction(expectedDigest.getDigestFunction())
            .build();

    return transform(
        deadlined(fetchFutureStub)
            .withInterceptors(attachMetadataInterceptor(requestMetadata))
            .fetchBlob(request),
        response -> {
          if (Code.forNumber(response.getStatus().getCode()) != Code.OK) {
            throw StatusProto.toStatusRuntimeException(response.getStatus());
          }
          // other responses, uris, expirations, etc
          return DigestUtil.fromDigest(
              response.getBlobDigest(), expectedDigest.getDigestFunction());
        },
        directExecutor());
  }

  @Override
  public boolean isReadOnly() {
    return false;
  }

  @Override
  public Write getOperationStreamWrite(String name) {
    return getWrite(
        name,
        e -> e,
        StubWriteOutputStream.UNLIMITED_EXPECTED_SIZE,
        /* autoflush= */ true,
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
        identifier,
        () -> deadlined(bsStub).withInterceptors(attachMetadataInterceptor(requestMetadata)),
        retrier::newBackoff,
        retrier::isRetriable,
        retryService);
  }

  @Override
  public String readResourceName(
      Compressor.Value compressor, build.buildfarm.v1test.Digest blobDigest) {
    return ResourceParser.downloadResourceName(
        DownloadBlobRequest.newBuilder()
            .setInstanceName(getName())
            .setBlob(BlobInformation.newBuilder().setCompressor(compressor).setDigest(blobDigest))
            .build());
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
      Compressor.Value compressor,
      build.buildfarm.v1test.Digest blobDigest,
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
                .setResourceName(readResourceName(compressor, blobDigest))
                .setReadOffset(offset)
                .setReadLimit(limit)
                .build(),
            new ReadBlobInterchange(blobObserver));
  }

  @Override
  public InputStream newBlobInput(
      Compressor.Value compressor,
      build.buildfarm.v1test.Digest digest,
      long offset,
      long deadlineAfter,
      TimeUnit deadlineAfterUnits,
      RequestMetadata requestMetadata)
      throws IOException {
    return newInput(readResourceName(compressor, digest), offset, requestMetadata);
  }

  @Override
  public ListenableFuture<List<Response>> getAllBlobsFuture(
      Iterable<Digest> digests, DigestFunction.Value digestFunction) {
    return transform(
        deadlined(casFutureStub)
            .batchReadBlobs(
                BatchReadBlobsRequest.newBuilder()
                    .setInstanceName(getName())
                    .addAllDigests(digests)
                    .setDigestFunction(digestFunction)
                    .build()),
        BatchReadBlobsResponse::getResponsesList,
        directExecutor());
  }

  @Override
  public boolean containsBlob(
      build.buildfarm.v1test.Digest digest,
      Digest.Builder result,
      RequestMetadata requestMetadata) {
    Digest blobDigest = DigestUtil.toDigest(digest);
    result.mergeFrom(blobDigest);
    try {
      return Iterables.isEmpty(
          findMissingBlobs(
                  ImmutableList.of(blobDigest), digest.getDigestFunction(), requestMetadata)
              .get());
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
                ByteStreamGrpc.newStub(writeChannel)
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
  public Write getBlobWrite(
      Compressor.Value compressor,
      build.buildfarm.v1test.Digest digest,
      UUID uuid,
      RequestMetadata requestMetadata) {
    BlobInformation blob =
        BlobInformation.newBuilder().setCompressor(compressor).setDigest(digest).build();
    String resourceName =
        ResourceParser.uploadResourceName(
            UploadBlobRequest.newBuilder()
                .setInstanceName(getName())
                .setBlob(blob)
                .setUuid(uuid.toString())
                .build());
    return getWrite(
        resourceName,
        t -> {
          Status status = Status.fromThrowable(t);
          if (status.getCode() == Status.Code.OUT_OF_RANGE) {
            t = new EntryLimitException(status.getDescription());
          }
          return t;
        },
        compressor == Compressor.Value.IDENTITY
            ? digest.getSize()
            : StubWriteOutputStream.COMPRESSED_EXPECTED_SIZE,
        /* autoflush= */ false,
        requestMetadata);
  }

  @Override
  public String getTree(
      build.buildfarm.v1test.Digest rootDigest, int pageSize, String pageToken, Tree.Builder tree) {
    DigestUtil digestUtil = new DigestUtil(HashFunction.get(rootDigest.getDigestFunction()));
    tree.setRootDigest(rootDigest);
    throwIfStopped();
    Iterator<GetTreeResponse> replies =
        deadlined(casBlockingStub)
            .getTree(
                GetTreeRequest.newBuilder()
                    .setInstanceName(getName())
                    .setRootDigest(DigestUtil.toDigest(rootDigest))
                    .setDigestFunction(rootDigest.getDigestFunction())
                    .setPageSize(pageSize)
                    .setPageToken(pageToken)
                    .build());
    // new streaming interface doesn't really fit with what we're trying to do here...
    String nextPageToken = Instance.SENTINEL_PAGE_TOKEN;
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
      // TODO should this be ActionKey
      build.buildfarm.v1test.Digest actionDigest,
      boolean skipCacheLookup,
      ExecutionPolicy executionPolicy,
      ResultsCachePolicy resultsCachePolicy,
      RequestMetadata metadata,
      Watcher watcher) {
    throw new UnsupportedOperationException();
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
      log.log(
          Level.SEVERE,
          format("putOperation(%s) response was unexpected", operation.getName()),
          StatusProto.toStatusException(status));
    }
    return code == Code.OK.getNumber();
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
      log.log(
          Level.SEVERE,
          format("pollOperation(%s) response was unexpected", operationName),
          StatusProto.toStatusException(status));
    }
    return code == Code.OK.getNumber();
  }

  @Override
  public ListenableFuture<Void> watchExecution(UUID executionId, Watcher watcher) {
    WaitExecutionRequest request =
        WaitExecutionRequest.newBuilder().setName(bindExecutions(executionId)).build();
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
      String name, int pageSize, String pageToken, String filter, Consumer<Operation> onOperation) {
    throwIfStopped();
    ListOperationsResponse response =
        deadlined(operationsBlockingStub)
            .listOperations(
                ListOperationsRequest.newBuilder()
                    .setName(getName() + "/" + name)
                    .setPageSize(pageSize)
                    .setPageToken(pageToken)
                    .setFilter(filter)
                    .build());
    response.getOperationsList().forEach(onOperation);
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
  public ListenableFuture<WorkerProfileMessage> getWorkerProfile(String name) {
    return deadlined(workerProfileFutureStub)
        .getWorkerProfile(WorkerProfileRequest.newBuilder().setWorkerName(name).build());
  }

  @Override
  public ListenableFuture<BatchWorkerProfilesResponse> batchWorkerProfiles(Iterable<String> names) {
    return deadlined(workerProfileFutureStub)
        .batchWorkerProfiles(
            BatchWorkerProfilesRequest.newBuilder()
                .setInstanceName(getName())
                .addAllWorkerNames(names)
                .build());
  }

  @Override
  public GetClientStartTimeResult getClientStartTime(GetClientStartTimeRequest request) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CasIndexResults reindexCas() {
    throwIfStopped();
    ReindexCasRequestResults proto =
        adminBlockingStub.get().reindexCas(ReindexCasRequest.newBuilder().build());
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
  public PrepareWorkerForGracefulShutDownRequestResults shutDownWorkerGracefully(String name) {
    throwIfStopped();
    return workerControlBlockingStub
        .get()
        .prepareWorkerForGracefulShutdown(
            PrepareWorkerForGracefulShutDownRequest.newBuilder().build());
  }

  @Override
  public ListenableFuture<WorkerPipelineChangeResponse> pipelineChange(
      String name, List<PipelineChange> changes) {
    throwIfStopped();
    SettableFuture<WorkerPipelineChangeResponse> result = SettableFuture.create();

    result.set(
        deadlined(workerControlBlockingStub)
            .pipelineChange(
                WorkerPipelineChangeRequest.newBuilder()
                    .setWorkerName(name)
                    .addAllChanges(changes)
                    .build()));
    return result;
  }
}
