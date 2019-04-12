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

import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.logging.Level.SEVERE;

import build.bazel.remote.execution.v2.ActionCacheGrpc;
import build.bazel.remote.execution.v2.ActionCacheGrpc.ActionCacheBlockingStub;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.BatchReadBlobsRequest;
import build.bazel.remote.execution.v2.BatchReadBlobsResponse.Response;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc.ContentAddressableStorageBlockingStub;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc.ContentAddressableStorageFutureStub;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecutionPolicy;
import build.bazel.remote.execution.v2.FindMissingBlobsRequest;
import build.bazel.remote.execution.v2.FindMissingBlobsResponse;
import build.bazel.remote.execution.v2.GetTreeRequest;
import build.bazel.remote.execution.v2.GetTreeResponse;
import build.bazel.remote.execution.v2.Platform;
import build.bazel.remote.execution.v2.ResultsCachePolicy;
import build.bazel.remote.execution.v2.ServerCapabilities;
import build.bazel.remote.execution.v2.UpdateActionResultRequest;
import build.buildfarm.common.Write;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.function.InterruptingPredicate;
import build.buildfarm.common.grpc.ByteStreamHelper;
import build.buildfarm.common.grpc.Retrier;
import build.buildfarm.common.grpc.StubWriteOutputStream;
import build.buildfarm.instance.Instance;
import build.buildfarm.v1test.OperationQueueGrpc;
import build.buildfarm.v1test.OperationQueueGrpc.OperationQueueBlockingStub;
import build.buildfarm.v1test.PollOperationRequest;
import build.buildfarm.v1test.TakeOperationRequest;
import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamGrpc.ByteStreamBlockingStub;
import com.google.bytestream.ByteStreamGrpc.ByteStreamStub;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.hash.HashCode;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.logging.Logger;
import javax.annotation.Nullable;

public class StubInstance implements Instance {
  private final static Logger logger = Logger.getLogger(StubInstance.class.getName());

  private final String name;
  private final DigestUtil digestUtil;
  private final Channel channel;
  private final ByteStreamUploader uploader;
  private final Retrier retrier;
  private final @Nullable ListeningScheduledExecutorService retryService;

  public StubInstance(
      String name,
      DigestUtil digestUtil,
      Channel channel,
      ByteStreamUploader uploader,
      Retrier retrier,
      ListeningScheduledExecutorService retryService) {
    this.name = name;
    this.digestUtil = digestUtil;
    this.channel = channel;
    this.uploader = uploader;
    this.retrier = retrier;
    this.retryService = retryService;
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
  public ActionResult getActionResult(ActionKey actionKey) {
    return null;
  }

  @Override
  public void putActionResult(ActionKey actionKey, ActionResult actionResult) {
    // should we be checking the ActionResult return value?
    actionCacheBlockingStub.get().updateActionResult(UpdateActionResultRequest.newBuilder()
        .setInstanceName(getName())
        .setActionDigest(actionKey.getDigest())
        .setActionResult(actionResult)
        .build());
  }

  @Override
  public Iterable<Digest> findMissingBlobs(Iterable<Digest> digests) {
    FindMissingBlobsResponse response = casBlockingStub.get()
        .findMissingBlobs(FindMissingBlobsRequest.newBuilder()
            .setInstanceName(getName())
            .addAllBlobDigests(digests)
            .build());
    return response.getMissingBlobDigestsList();
  }

  @Override
  public Iterable<Digest> putAllBlobs(Iterable<ByteString> blobs)
      throws IOException, IllegalArgumentException, InterruptedException {
    // sort of a blatant misuse - one chunker per input, query digests before exhausting iterators
    Map<HashCode, Chunker> chunkers = Maps.newHashMap();
    ImmutableList.Builder<Digest> digests = ImmutableList.builder();
    for (ByteString blob : blobs) {
      Chunker chunker = Chunker.builder().setInput(blob).build();
      Digest digest = digestUtil.compute(blob);
      digests.add(digest);
      chunkers.put(HashCode.fromString(digest.getHash()), chunker);
    }
    uploader.uploadBlobs(chunkers);
    return digests.build();
  }

  @Override
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
        bsStub,
        retrier::newBackoff,
        retrier::isRetriable,
        retryService);
  }

  @Override
  public String getBlobName(Digest blobDigest) {
    return String.format(
        "%s/blobs/%s",
        getName(),
        DigestUtil.toString(blobDigest));
  }

  @Override
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
  public ByteString getBlob(Digest blobDigest) {
    if (blobDigest.getSizeBytes() == 0) {
      return ByteString.EMPTY;
    }
    try (InputStream in = newInput(getBlobName(blobDigest), /* offset=*/ 0)) {
      return ByteString.readFrom(in);
    } catch (IOException ex) {
      return null;
    }
  }

  @Override
  public ByteString getBlob(Digest blobDigest, long offset, long limit) {
    return null;
  }

  @Override
  public boolean containsBlob(Digest digest) {
    return Iterables.isEmpty(findMissingBlobs(ImmutableList.of(digest)));
  }

  Write getWrite(String resourceName, long expectedSize, boolean autoflush) {
    return new StubWriteOutputStream(
        bsBlockingStub,
        bsStub,
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
    String resourceName = ByteStreamUploader.uploadResourceName(
        getName(),
        uuid,
        HashCode.fromString(digest.getHash()),
        digest.getSizeBytes());
    return getWrite(resourceName, digest.getSizeBytes(), /* autoflush=*/ false);
  }

  @Override
  public Digest putBlob(ByteString blob)
      throws IOException, IllegalArgumentException, InterruptedException {
    if (blob.size() == 0) {
      return digestUtil.empty();
    }
    Chunker chunker = Chunker.builder().setInput(blob).build();
    Digest digest = digestUtil.compute(blob);
    uploader.uploadBlob(HashCode.fromString(digest.getHash()), chunker);
    return digest;
  }

  @Override
  public String getTree(
      Digest rootDigest,
      int pageSize,
      String pageToken,
      ImmutableList.Builder<Directory> directories) {
    Iterator<GetTreeResponse> replies = casBlockingStub.get()
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
      Predicate<Operation> onOperation) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void match(Platform platform, InterruptingPredicate<Operation> onMatch) throws InterruptedException {
    Operation operation = operationQueueBlockingStub.get()
        .take(TakeOperationRequest.newBuilder()
        .setInstanceName(getName())
        .setPlatform(platform)
        .build());
    onMatch.test(operation);
  }

  @Override
  public boolean putOperation(Operation operation) {
    return operationQueueBlockingStub
        .get()
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
    return operationQueueBlockingStub
        .get()
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
    return false;
  }

  @Override
  public String listOperations(
      int pageSize, String pageToken, String filter,
      ImmutableList.Builder<Operation> operations) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Operation getOperation(String operationName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteOperation(String operationName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void cancelOperation(String operationName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ServerCapabilities getCapabilities() {
    throw new UnsupportedOperationException();
  }
}
