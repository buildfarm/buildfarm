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

import build.buildfarm.instance.Instance;
import com.google.common.collect.ImmutableList;
import build.bazel.remote.execution.v2.BatchUpdateBlobsRequest;
import build.bazel.remote.execution.v2.BatchUpdateBlobsRequest.Request;
import build.bazel.remote.execution.v2.BatchUpdateBlobsResponse;
import build.bazel.remote.execution.v2.BatchUpdateBlobsResponse.Response;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.FindMissingBlobsRequest;
import build.bazel.remote.execution.v2.FindMissingBlobsResponse;
import build.bazel.remote.execution.v2.GetTreeRequest;
import build.bazel.remote.execution.v2.GetTreeResponse;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.function.Function;

public class ContentAddressableStorageService extends ContentAddressableStorageGrpc.ContentAddressableStorageImplBase {
  private final Instances instances;

  public ContentAddressableStorageService(Instances instances) {
    this.instances = instances;
  }

  @Override
  public void findMissingBlobs(
      FindMissingBlobsRequest request,
      StreamObserver<FindMissingBlobsResponse> responseObserver) {
    Instance instance;
    try {
      instance = instances.get(request.getInstanceName());
    } catch (InstanceNotFoundException ex) {
      responseObserver.onError(BuildFarmInstances.toStatusException(ex));
      return;
    }

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
    Instance instance;
    try {
      instance = instances.get(batchRequest.getInstanceName());
    } catch (InstanceNotFoundException ex) {
      responseObserver.onError(BuildFarmInstances.toStatusException(ex));
      return;
    }

    ImmutableList.Builder<ByteString> validBlobsBuilder = new ImmutableList.Builder<>();
    ImmutableList.Builder<Response> responses =
        new ImmutableList.Builder<>();
    Function<com.google.rpc.Code, com.google.rpc.Status> statusForCode =
        (code) -> com.google.rpc.Status.newBuilder()
            .setCode(code.getNumber())
            .build();
    // FIXME do this validation through the instance interface
    for (Request request : batchRequest.getRequestsList()) {
      com.google.rpc.Status status;
      Digest digest = request.getDigest();
      if (digest.equals(instance.getDigestUtil().compute(request.getData()))) {
        validBlobsBuilder.add(request.getData());
        status = statusForCode.apply(com.google.rpc.Code.OK);
      } else {
        status = statusForCode.apply(com.google.rpc.Code.INVALID_ARGUMENT);
      }
      responses.add(Response.newBuilder()
          .setDigest(digest)
          .setStatus(status)
          .build());
    }

    try {
      instance.putAllBlobs(validBlobsBuilder.build());
    } catch (InterruptedException ex) {
      responseObserver.onError(new StatusException(Status.fromThrowable(ex)));
      Thread.currentThread().interrupt();
      return;
    } catch (IOException ex) {
      responseObserver.onError(new StatusException(Status.fromThrowable(ex)));
      return;
    }

    responseObserver.onNext(BatchUpdateBlobsResponse.newBuilder()
        .addAllResponses(responses.build())
        .build());
    responseObserver.onCompleted();
  }

  @Override
  public void getTree(
      GetTreeRequest request,
      StreamObserver<GetTreeResponse> responseObserver) {
    Instance instance;
    try {
      instance = instances.get(request.getInstanceName());
    } catch (InstanceNotFoundException ex) {
      responseObserver.onError(BuildFarmInstances.toStatusException(ex));
      return;
    }

    int pageSize = request.getPageSize();
    if (pageSize < 0) {
      responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
      return;
    }

    String pageToken = request.getPageToken();
    do {
      ImmutableList.Builder<Directory> directories = new ImmutableList.Builder<>();
      String nextPageToken = instance.getTree(
          request.getRootDigest(), pageSize, pageToken, directories);

      responseObserver.onNext(GetTreeResponse.newBuilder()
          .addAllDirectories(directories.build())
          .setNextPageToken(nextPageToken)
          .build());
      pageToken = nextPageToken;
    } while (!pageToken.isEmpty());
    responseObserver.onCompleted();
  }
}

