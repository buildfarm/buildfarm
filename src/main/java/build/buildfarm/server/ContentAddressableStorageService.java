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

package build.buildfarm.server;

import build.buildfarm.common.Digests;
import build.buildfarm.instance.Instance;
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
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.function.Function;

public class ContentAddressableStorageService extends ContentAddressableStorageGrpc.ContentAddressableStorageImplBase {
  private final BuildFarmServer server;

  public ContentAddressableStorageService(BuildFarmServer server) {
    this.server = server;
  }

  @Override
  public void findMissingBlobs(
      FindMissingBlobsRequest request,
      StreamObserver<FindMissingBlobsResponse> responseObserver) {
    Instance instance = server.getInstance(request.getInstanceName());

    responseObserver.onNext(FindMissingBlobsResponse.newBuilder()
        .addAllMissingBlobDigests(
            instance.findMissingBlobs(request.getBlobDigestsList()))
        .build());
    responseObserver.onCompleted();
  }

  @Override
  public void batchUpdateBlobs(
      BatchUpdateBlobsRequest batchRequest,
      StreamObserver<BatchUpdateBlobsResponse> responseObserver) {
    Instance instance = server.getInstance(batchRequest.getInstanceName());

    ImmutableList.Builder<ByteString> validBlobsBuilder = new ImmutableList.Builder<>();
    ImmutableList.Builder<BatchUpdateBlobsResponse.Response> responses =
        new ImmutableList.Builder<>();
    Function<com.google.rpc.Code, com.google.rpc.Status> statusForCode =
        (code) -> com.google.rpc.Status.newBuilder()
            .setCode(code.getNumber())
            .build();
    // FIXME do this validation through the instance interface
    for( UpdateBlobRequest request : batchRequest.getRequestsList() ) {
      com.google.rpc.Status status;
      Digest digest = request.getContentDigest();
      if (!request.getData().isEmpty() &&
          digest.equals(Digests.computeDigest(request.getData()))) {
        validBlobsBuilder.add(request.getData());
        status = statusForCode.apply(com.google.rpc.Code.OK);
      } else {
        status = statusForCode.apply(com.google.rpc.Code.INVALID_ARGUMENT);
      }
      responses.add(BatchUpdateBlobsResponse.Response.newBuilder()
          .setBlobDigest(digest)
          .setStatus(status)
          .build());
    }

    try {
      instance.putAllBlobs(validBlobsBuilder.build());
      responseObserver.onNext(BatchUpdateBlobsResponse.newBuilder()
          .addAllResponses(responses.build())
          .build());
      responseObserver.onCompleted();
    } catch(InterruptedException ex) {
      responseObserver.onError(ex);
    }
  }

  @Override
  public void getTree(
      GetTreeRequest request,
      StreamObserver<GetTreeResponse> responseObserver) {
    Instance instance = server.getInstance(request.getInstanceName());

    int pageSize = request.getPageSize();
    if (pageSize < 0) {
      responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
      return;
    }
    ImmutableList.Builder<Directory> directories = new ImmutableList.Builder<>();
    String nextPageToken = instance.getTree(
        request.getRootDigest(), pageSize, request.getPageToken(), directories);

    responseObserver.onNext(GetTreeResponse.newBuilder()
        .addAllDirectories(directories.build())
        .setNextPageToken(nextPageToken)
        .build());
    responseObserver.onCompleted();
  }
}

