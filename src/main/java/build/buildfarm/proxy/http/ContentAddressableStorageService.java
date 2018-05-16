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

import build.buildfarm.common.TokenizableIterator;
import build.buildfarm.common.TreeIterator;
import com.google.common.collect.ImmutableList;
import com.google.devtools.remoteexecution.v1test.BatchUpdateBlobsRequest;
import com.google.devtools.remoteexecution.v1test.BatchUpdateBlobsResponse;
import com.google.devtools.remoteexecution.v1test.ContentAddressableStorageGrpc;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.Directory;
import com.google.devtools.remoteexecution.v1test.FindMissingBlobsRequest;
import com.google.devtools.remoteexecution.v1test.FindMissingBlobsResponse;
import com.google.devtools.remoteexecution.v1test.GetTreeRequest;
import com.google.devtools.remoteexecution.v1test.GetTreeResponse;
import com.google.devtools.remoteexecution.v1test.UpdateBlobRequest;
import com.google.protobuf.ByteString;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.function.Function;

public class ContentAddressableStorageService extends ContentAddressableStorageGrpc.ContentAddressableStorageImplBase {
  private final SimpleBlobStore simpleBlobStore;
  private final int treeDefaultPageSize;
  private final int treeMaxPageSize;

  public ContentAddressableStorageService(
      SimpleBlobStore simpleBlobStore,
      int treeDefaultPageSize,
      int treeMaxPageSize) {
    this.simpleBlobStore = simpleBlobStore;
    this.treeDefaultPageSize = treeDefaultPageSize;
    this.treeMaxPageSize = treeMaxPageSize;
  }

  @Override
  public void findMissingBlobs(
      FindMissingBlobsRequest request,
      StreamObserver<FindMissingBlobsResponse> responseObserver) {
    FindMissingBlobsResponse.Builder responseBuilder =
        FindMissingBlobsResponse.newBuilder();
    try {
      for (Digest blobDigest : request.getBlobDigestsList()) {
        if (!simpleBlobStore.containsKey(blobDigest.getHash())) {
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
    ImmutableList.Builder<ByteString> validBlobsBuilder = new ImmutableList.Builder<>();
    ImmutableList.Builder<BatchUpdateBlobsResponse.Response> responses =
        new ImmutableList.Builder<>();
    Function<com.google.rpc.Code, com.google.rpc.Status> statusForCode =
        (code) -> com.google.rpc.Status.newBuilder()
            .setCode(code.getNumber())
            .build();
    for (UpdateBlobRequest request : batchRequest.getRequestsList()) {
      Digest digest = request.getContentDigest();
      try {
        simpleBlobStore.put(
            digest.getHash(),
            digest.getSizeBytes(),
            request.getData().newInput());
        responses.add(BatchUpdateBlobsResponse.Response.newBuilder()
            .setBlobDigest(digest)
            .setStatus(statusForCode.apply(com.google.rpc.Code.OK))
            .build());
      } catch (IOException e) {
        StatusException statusException = Status.fromThrowable(e).asException();
        responses.add(BatchUpdateBlobsResponse.Response.newBuilder()
            .setBlobDigest(digest)
            .setStatus(StatusProto.fromThrowable(statusException))
            .build());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }

    responseObserver.onNext(BatchUpdateBlobsResponse.newBuilder()
        .addAllResponses(responses.build())
        .build());
    responseObserver.onCompleted();
  }

  private String getTree(
      Digest rootDigest, int pageSize, String pageToken,
      ImmutableList.Builder<Directory> directories)
      throws IOException, InterruptedException {
    if (pageSize == 0) {
      pageSize = treeDefaultPageSize;
    }
    if (pageSize >= 0 && pageSize > treeMaxPageSize) {
      pageSize = treeMaxPageSize;
    }

    TokenizableIterator<Directory> iter =
        new TreeIterator(
            (digest) -> {
              ByteArrayOutputStream stream = new ByteArrayOutputStream((int) digest.getSizeBytes());
              if (!simpleBlobStore.get(digest.getHash(), stream)) {
                return null;
              }
              return ByteString.copyFrom(stream.toByteArray());
            }, rootDigest, pageToken);

    while (iter.hasNext() && pageSize != 0) {
      Directory directory = iter.next();
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
  public void getTree(
      GetTreeRequest request,
      StreamObserver<GetTreeResponse> responseObserver) {
    int pageSize = request.getPageSize();
    if (pageSize < 0) {
      responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
      return;
    }
    ImmutableList.Builder<Directory> directories = new ImmutableList.Builder<>();
    try {
      String nextPageToken = getTree(
          request.getRootDigest(),
          pageSize,
          request.getPageToken(),
          directories);

      responseObserver.onNext(GetTreeResponse.newBuilder()
          .addAllDirectories(directories.build())
          .setNextPageToken(nextPageToken)
          .build());
      responseObserver.onCompleted();
    } catch (IOException e) {
      responseObserver.onError(Status.fromThrowable(e).asException());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
