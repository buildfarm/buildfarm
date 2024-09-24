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

package build.buildfarm.proxy.http;

import static build.buildfarm.common.DigestUtil.optionalDigestFunction;
import static com.google.common.util.concurrent.Futures.catching;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import build.bazel.remote.execution.v2.BatchUpdateBlobsRequest;
import build.bazel.remote.execution.v2.BatchUpdateBlobsRequest.Request;
import build.bazel.remote.execution.v2.BatchUpdateBlobsResponse;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.FindMissingBlobsRequest;
import build.bazel.remote.execution.v2.FindMissingBlobsResponse;
import build.bazel.remote.execution.v2.GetTreeRequest;
import build.bazel.remote.execution.v2.GetTreeResponse;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.TokenizableIterator;
import build.buildfarm.common.TreeIterator;
import build.buildfarm.common.TreeIterator.DirectoryEntry;
import build.buildfarm.v1test.Digest;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class ContentAddressableStorageService
    extends ContentAddressableStorageGrpc.ContentAddressableStorageImplBase {
  private final SimpleBlobStore simpleBlobStore;
  private final int treeDefaultPageSize;
  private final int treeMaxPageSize;

  public ContentAddressableStorageService(
      SimpleBlobStore simpleBlobStore, int treeDefaultPageSize, int treeMaxPageSize) {
    this.simpleBlobStore = simpleBlobStore;
    this.treeDefaultPageSize = treeDefaultPageSize;
    this.treeMaxPageSize = treeMaxPageSize;
  }

  private static String key(
      build.bazel.remote.execution.v2.Digest digest, DigestFunction.Value digestFunction) {
    Digest blobDigest = DigestUtil.fromDigest(digest, digestFunction);
    return digestKey(blobDigest);
  }

  public static String digestKey(Digest digest) {
    return optionalDigestFunction(digest.getDigestFunction()) + digest.getHash();
  }

  @Override
  public void findMissingBlobs(
      FindMissingBlobsRequest request, StreamObserver<FindMissingBlobsResponse> responseObserver) {
    FindMissingBlobsResponse.Builder responseBuilder = FindMissingBlobsResponse.newBuilder();
    try {
      for (build.bazel.remote.execution.v2.Digest blobDigest : request.getBlobDigestsList()) {
        if (!simpleBlobStore.containsKey(key(blobDigest, request.getDigestFunction()))) {
          responseBuilder.addMissingBlobDigests(blobDigest);
        }
      }
      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    } catch (IOException e) {
      responseObserver.onError(Status.fromThrowable(e).asException());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void batchUpdateBlobs(
      BatchUpdateBlobsRequest batchRequest,
      StreamObserver<BatchUpdateBlobsResponse> responseObserver) {
    ImmutableList.Builder<BatchUpdateBlobsResponse.Response> responses =
        new ImmutableList.Builder<>();
    Function<com.google.rpc.Code, com.google.rpc.Status> statusForCode =
        (code) -> com.google.rpc.Status.newBuilder().setCode(code.getNumber()).build();
    for (Request request : batchRequest.getRequestsList()) {
      build.bazel.remote.execution.v2.Digest digest = request.getDigest();
      try {
        simpleBlobStore.put(
            key(digest, batchRequest.getDigestFunction()),
            digest.getSizeBytes(),
            request.getData().newInput());
        responses.add(
            BatchUpdateBlobsResponse.Response.newBuilder()
                .setDigest(digest)
                .setStatus(statusForCode.apply(com.google.rpc.Code.OK))
                .build());
      } catch (IOException e) {
        StatusException statusException = Status.fromThrowable(e).asException();
        responses.add(
            BatchUpdateBlobsResponse.Response.newBuilder()
                .setDigest(digest)
                .setStatus(StatusProto.fromThrowable(statusException))
                .build());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }

    responseObserver.onNext(
        BatchUpdateBlobsResponse.newBuilder().addAllResponses(responses.build()).build());
    responseObserver.onCompleted();
  }

  private String getTree(
      Digest rootDigest,
      int pageSize,
      String pageToken,
      ImmutableList.Builder<Directory> directories) {
    if (pageSize == 0) {
      pageSize = treeDefaultPageSize;
    }
    if (pageSize >= 0 && pageSize > treeMaxPageSize) {
      pageSize = treeMaxPageSize;
    }

    TokenizableIterator<DirectoryEntry> iter =
        new TreeIterator(
            (digest) -> {
              ByteString.Output stream = ByteString.newOutput((int) digest.getSize());
              try {
                return transform(
                        catching(
                            simpleBlobStore.get(digestKey(digest), stream),
                            Exception.class,
                            (e) -> false,
                            directExecutor()),
                        (success) -> {
                          if (success) {
                            try {
                              return Directory.parseFrom(stream.toByteString());
                            } catch (InvalidProtocolBufferException e) {
                              // ignore
                            }
                          }
                          return null;
                        },
                        directExecutor())
                    .get();
              } catch (ExecutionException e) {
                Throwables.propagateIfInstanceOf(e.getCause(), Error.class);
                return null; // should not happen due to catching
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
              }
            },
            rootDigest,
            pageToken);

    while (iter.hasNext() && pageSize != 0) {
      Directory directory = iter.next().getDirectory();
      // If part of the tree is missing from the CAS, the server will return the
      // portion present and omit the rest.
      if (directory != null) {
        directories.add(directory);
        if (pageSize > 0) {
          pageSize--;
        }
      }
    }
    return iter.toNextPageToken();
  }

  @Override
  public void getTree(GetTreeRequest request, StreamObserver<GetTreeResponse> responseObserver) {
    int pageSize = request.getPageSize();
    if (pageSize < 0) {
      responseObserver.onError(Status.INVALID_ARGUMENT.asException());
      return;
    }
    ImmutableList.Builder<Directory> directories = new ImmutableList.Builder<>();
    String nextPageToken =
        getTree(
            DigestUtil.fromDigest(request.getRootDigest(), request.getDigestFunction()),
            pageSize,
            request.getPageToken(),
            directories);

    responseObserver.onNext(
        GetTreeResponse.newBuilder()
            .addAllDirectories(directories.build())
            .setNextPageToken(nextPageToken)
            .build());
    responseObserver.onCompleted();
  }
}
