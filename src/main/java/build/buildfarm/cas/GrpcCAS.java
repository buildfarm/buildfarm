// Copyright 2018 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.cas;

import static build.buildfarm.common.grpc.Retrier.NO_RETRIES;
import static build.buildfarm.common.grpc.TracingMetadataUtils.attachMetadataInterceptor;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import build.bazel.remote.execution.v2.BatchReadBlobsRequest;
import build.bazel.remote.execution.v2.BatchReadBlobsResponse;
import build.bazel.remote.execution.v2.BatchReadBlobsResponse.Response;
import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc.ContentAddressableStorageBlockingStub;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc.ContentAddressableStorageFutureStub;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.FindMissingBlobsRequest;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.Write;
import build.buildfarm.common.grpc.ByteStreamHelper;
import build.buildfarm.common.grpc.DelegateServerCallStreamObserver;
import build.buildfarm.common.grpc.StubWriteOutputStream;
import build.buildfarm.common.resources.BlobInformation;
import build.buildfarm.common.resources.DownloadBlobRequest;
import build.buildfarm.common.resources.ResourceParser;
import build.buildfarm.common.resources.UploadBlobRequest;
import build.buildfarm.instance.stub.ByteStreamUploader;
import build.buildfarm.instance.stub.Chunker;
import build.buildfarm.v1test.Digest;
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
import com.google.common.collect.ListMultimap;
import com.google.common.hash.HashCode;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;

public class GrpcCAS implements ContentAddressableStorage {
  private final String instanceName;
  private final boolean readonly;
  private final Channel channel;
  private final ByteStreamUploader uploader;
  private final ListMultimap<Digest, Runnable> onExpirations;

  GrpcCAS(
      String instanceName,
      boolean readonly,
      Channel channel,
      ByteStreamUploader uploader,
      ListMultimap<Digest, Runnable> onExpirations) {
    this.instanceName = instanceName;
    this.readonly = readonly;
    this.channel = channel;
    this.uploader = uploader;
    this.onExpirations = onExpirations;
  }

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
  private final Supplier<ContentAddressableStorageFutureStub> casFutureStub =
      Suppliers.memoize(
          new Supplier<>() {
            @Override
            public ContentAddressableStorageFutureStub get() {
              return ContentAddressableStorageGrpc.newFutureStub(channel);
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

  private InputStream newStreamInput(String resourceName, long offset) throws IOException {
    return ByteStreamHelper.newInput(
        resourceName,
        offset,
        channel.toString(),
        bsStub,
        NO_RETRIES::newBackoff,
        NO_RETRIES::isRetriable,
        /* retryService= */ null);
  }

  private String readResourceName(Compressor.Value compressor, Digest digest) {
    return ResourceParser.downloadResourceName(
        DownloadBlobRequest.newBuilder()
            .setInstanceName(instanceName)
            .setBlob(BlobInformation.newBuilder().setCompressor(compressor).setDigest(digest))
            .build());
  }

  @Override
  public boolean contains(Digest digest, build.bazel.remote.execution.v2.Digest.Builder result) {
    // QueryWriteStatusRequest?
    if (digest.getSize() < 0) {
      throw new UnsupportedOperationException("cannot lookup hash without size via grpc cas");
    }
    build.bazel.remote.execution.v2.Digest blobDigest = DigestUtil.toDigest(digest);
    result.mergeFrom(blobDigest);
    return Iterables.isEmpty(
        findMissingBlobs(ImmutableList.of(blobDigest), digest.getDigestFunction()));
  }

  @Override
  public Iterable<build.bazel.remote.execution.v2.Digest> findMissingBlobs(
      Iterable<build.bazel.remote.execution.v2.Digest> digests,
      DigestFunction.Value digestFunction) {
    digests =
        StreamSupport.stream(digests.spliterator(), false)
            .filter(digest -> digest.getSizeBytes() != 0)
            .collect(Collectors.toList());
    if (Iterables.isEmpty(digests)) {
      return ImmutableList.of();
    }

    List<build.bazel.remote.execution.v2.Digest> missingDigests =
        casBlockingStub
            .get()
            .findMissingBlobs(
                FindMissingBlobsRequest.newBuilder()
                    .setInstanceName(instanceName)
                    .addAllBlobDigests(digests)
                    .setDigestFunction(digestFunction)
                    .build())
            .getMissingBlobDigestsList();
    for (build.bazel.remote.execution.v2.Digest missingDigest : missingDigests) {
      expire(DigestUtil.fromDigest(missingDigest, digestFunction));
    }
    return missingDigests;
  }

  private void expire(Digest digest) {
    List<Runnable> digestOnExpirations;
    synchronized (onExpirations) {
      digestOnExpirations = onExpirations.removeAll(digest);
    }
    for (Runnable r : digestOnExpirations) {
      r.run();
    }
  }

  @Override
  public void get(
      Compressor.Value compressor,
      build.buildfarm.v1test.Digest digest,
      long offset,
      long count,
      ServerCallStreamObserver<ByteString> blobObserver,
      RequestMetadata requestMetadata) {
    ReadRequest request =
        ReadRequest.newBuilder()
            .setResourceName(readResourceName(compressor, digest))
            .setReadOffset(offset)
            .setReadLimit(count)
            .build();
    ByteStreamGrpc.newStub(channel)
        .withInterceptors(attachMetadataInterceptor(requestMetadata))
        .read(
            request,
            new DelegateServerCallStreamObserver<ReadResponse, ByteString>(blobObserver) {
              @Override
              public void onNext(ReadResponse response) {
                blobObserver.onNext(response.getData());
              }

              @Override
              public void onError(Throwable t) {
                blobObserver.onError(t);
              }

              @Override
              public void onCompleted() {
                blobObserver.onCompleted();
              }
            });
  }

  @Override
  public InputStream newInput(Compressor.Value compressor, Digest digest, long offset)
      throws IOException {
    return newStreamInput(readResourceName(compressor, digest), offset);
  }

  @Override
  public ListenableFuture<List<Response>> getAllFuture(
      Iterable<build.bazel.remote.execution.v2.Digest> digests,
      DigestFunction.Value digestFunction) {
    // FIXME limit to 4MiB total response
    return transform(
        casFutureStub
            .get()
            .batchReadBlobs(
                BatchReadBlobsRequest.newBuilder()
                    .setInstanceName(instanceName)
                    .addAllDigests(digests)
                    .setDigestFunction(digestFunction)
                    .build()),
        BatchReadBlobsResponse::getResponsesList,
        directExecutor());
  }

  @Override
  public Blob get(Digest digest) {
    try (InputStream in =
        newStreamInput(readResourceName(Compressor.Value.IDENTITY, digest), /* offset= */ 0)) {
      ByteString content = ByteString.readFrom(in);
      if (content.size() != digest.getSize()) {
        throw new IOException(
            String.format(
                "size/data mismatch: was %d, expected %d", content.size(), digest.getSize()));
      }
      return new Blob(content, digest);
    } catch (IOException ex) {
      expire(digest);
      return null;
    }
  }

  @SuppressWarnings("Guava")
  public static Write newWrite(
      Channel channel,
      String instanceName,
      Compressor.Value compressor,
      build.buildfarm.v1test.Digest digest,
      UUID uuid,
      RequestMetadata requestMetadata) {
    String resourceName =
        ResourceParser.uploadResourceName(
            UploadBlobRequest.newBuilder()
                .setInstanceName(instanceName)
                .setUuid(uuid.toString())
                .setBlob(BlobInformation.newBuilder().setDigest(digest).setCompressor(compressor))
                .build());
    Supplier<ByteStreamBlockingStub> bsBlockingStub =
        Suppliers.memoize(
            () ->
                ByteStreamGrpc.newBlockingStub(channel)
                    .withInterceptors(attachMetadataInterceptor(requestMetadata)));
    Supplier<ByteStreamStub> bsStub =
        Suppliers.memoize(
            () ->
                ByteStreamGrpc.newStub(channel)
                    .withInterceptors(attachMetadataInterceptor(requestMetadata)));
    return new StubWriteOutputStream(
        bsBlockingStub,
        bsStub,
        resourceName,
        Functions.identity(),
        digest.getSize(),
        /* autoflush= */ false);
  }

  @Override
  @Nullable
  public Write getWrite(
      Compressor.Value compressor, Digest digest, UUID uuid, RequestMetadata requestMetadata) {
    if (readonly) {
      return null;
    }
    return newWrite(channel, instanceName, compressor, digest, uuid, requestMetadata);
  }

  @Override
  public void put(Blob blob) throws InterruptedException {
    Chunker chunker = Chunker.builder().setInput(blob.getData()).build();
    try {
      uploader.uploadBlob(HashCode.fromString(blob.getDigest().getHash()), chunker);
    } catch (IOException e) {
      if (e.getCause() instanceof StatusRuntimeException) {
        throw (StatusRuntimeException) e.getCause();
      }
      throw new RuntimeException(e);
    }
  }

  @Override
  public void put(Blob blob, Runnable onExpiration) throws InterruptedException {
    try {
      put(blob);
    } catch (RuntimeException e) {
      onExpiration.run();
      throw e;
    }
    synchronized (onExpirations) {
      onExpirations.put(blob.getDigest(), onExpiration);
    }
  }

  @Override
  public long maxEntrySize() {
    return UNLIMITED_ENTRY_SIZE_MAX;
  }
}
