// Copyright 2018 The Bazel Authors. All rights reserved.
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
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import build.bazel.remote.execution.v2.BatchReadBlobsRequest;
import build.bazel.remote.execution.v2.BatchReadBlobsResponse.Response;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc.ContentAddressableStorageBlockingStub;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc.ContentAddressableStorageFutureStub;
import build.bazel.remote.execution.v2.FindMissingBlobsRequest;
import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.Write;
import build.buildfarm.common.grpc.ByteStreamHelper;
import build.buildfarm.common.grpc.RetryException;
import build.buildfarm.common.grpc.StubWriteOutputStream;
import build.buildfarm.instance.stub.ByteStreamUploader;
import build.buildfarm.instance.stub.Chunker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamGrpc.ByteStreamBlockingStub;
import com.google.bytestream.ByteStreamGrpc.ByteStreamStub;
import com.google.bytestream.ByteStreamProto.ReadRequest;
import com.google.bytestream.ByteStreamProto.ReadResponse;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Iterators;
import com.google.common.collect.ListMultimap;
import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

public class GrpcCAS implements ContentAddressableStorage {
  private final String instanceName;
  private final Channel channel;
  private final ByteStreamUploader uploader;
  private final ListMultimap<Digest, Runnable> onExpirations;

  GrpcCAS(String instanceName, Channel channel, ByteStreamUploader uploader, ListMultimap<Digest, Runnable> onExpirations) {
    this.instanceName = instanceName;
    this.channel = channel;
    this.uploader = uploader;
    this.onExpirations = onExpirations;
  }

  private final Supplier<ContentAddressableStorageBlockingStub> casBlockingStub =
      Suppliers.memoize(
          new Supplier<ContentAddressableStorageBlockingStub>() {
            @Override
            public ContentAddressableStorageBlockingStub get() {
              return ContentAddressableStorageGrpc.newBlockingStub(channel);
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

  private final Supplier<ByteStreamStub> bsStub =
      Suppliers.memoize(
          new Supplier<ByteStreamStub>() {
            @Override
            public ByteStreamStub get() {
              return ByteStreamGrpc.newStub(channel);
            }
          });

  private InputStream newStreamInput(String resourceName, long offset) {
    return ByteStreamHelper.newInput(
        resourceName,
        offset,
        bsStub,
        NO_RETRIES::newBackoff,
        NO_RETRIES::isRetriable,
        /* retryService=*/ null);
  }

  private String getBlobName(Digest digest) {
    return String.format(
        "%sblobs/%s",
        instanceName.isEmpty() ? "" : (instanceName + "/"),
        DigestUtil.toString(digest));
  }

  @Override
  public boolean contains(Digest digest) {
    // QueryWriteStatusRequest?
    return Iterables.isEmpty(findMissingBlobs(ImmutableList.of(digest)));
  }

  @Override
  public Iterable<Digest> findMissingBlobs(Iterable<Digest> digests) {
    List<Digest> missingDigests = casBlockingStub.get()
        .findMissingBlobs(FindMissingBlobsRequest.newBuilder()
            .setInstanceName(instanceName)
            .addAllBlobDigests(digests)
            .build())
        .getMissingBlobDigestsList();
    for (Digest missingDigest : missingDigests) {
      expire(missingDigest);
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
  public InputStream newInput(Digest digest, long offset) {
    return newStreamInput(getBlobName(digest), offset);
  }

  @Override
  public ListenableFuture<Iterable<Response>> getAllFuture(Iterable<Digest> digests) {
    // FIXME limit to 4MiB total response
    return transform(
        casFutureStub.get()
            .batchReadBlobs(BatchReadBlobsRequest.newBuilder()
                .setInstanceName(instanceName)
                .addAllDigests(digests)
                .build()),
        (response) -> response.getResponsesList(),
        directExecutor());
  }

  @Override
  public Blob get(Digest digest) {
    try (InputStream in = newStreamInput(getBlobName(digest), /* offset=*/ 0)) {
      ByteString content = ByteString.readFrom(in);
      if (content.size() != digest.getSizeBytes()) {
        throw new IOException(String.format(
            "size/data mismatch: was %d, expected %d",
            content.size(),
            digest.getSizeBytes()));
      }
      return new Blob(content, digest);
    } catch (IOException ex) {
      expire(digest);
      return null;
    }
  }

  public static Write newWrite(
      Channel channel,
      String instanceName,
      Digest digest,
      UUID uuid) {
    String resourceName = ByteStreamUploader.getResourceName(uuid, instanceName, digest);
    Supplier<ByteStreamBlockingStub> bsBlockingStub = Suppliers.memoize(
        () -> ByteStreamGrpc.newBlockingStub(channel));
    Supplier<ByteStreamStub> bsStub = Suppliers.memoize(
        () -> ByteStreamGrpc.newStub(channel));
    return new StubWriteOutputStream(
        bsBlockingStub,
        bsStub,
        resourceName,
        digest.getSizeBytes(),
        /* autoflush=*/ false);
  }

  @Override
  public Write getWrite(Digest digest, UUID uuid) {
    return newWrite(channel, instanceName, digest, uuid);
  }

  @Override
  public void put(Blob blob) throws InterruptedException {
    Chunker chunker = new Chunker(blob.getData(), blob.getDigest());
    try {
      uploader.uploadBlobs(Collections.singleton(chunker));
    } catch (RetryException e) {
      if (e.getCause() instanceof StatusRuntimeException) {
        throw (StatusRuntimeException) e.getCause();
      }
      throw new RuntimeException(e);
    }
  }

  @Override
  public void put(Blob blob, Runnable onExpiration) throws InterruptedException {
    put(blob);
    synchronized (onExpirations) {
      onExpirations.put(blob.getDigest(), onExpiration);
    }
  }
}
