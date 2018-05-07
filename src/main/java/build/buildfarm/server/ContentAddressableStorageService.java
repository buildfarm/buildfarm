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
    ImmutableList.Builder<BatchUpdateBlobsResponse.Response> responses =
        new ImmutableList.Builder<>();
    Function<com.google.rpc.Code, com.google.rpc.Status> statusForCode =
        (code) -> com.google.rpc.Status.newBuilder()
            .setCode(code.getNumber())
            .build();
    // FIXME do this validation through the instance interface
    for (UpdateBlobRequest request : batchRequest.getRequestsList()) {
      com.google.rpc.Status status;
      Digest digest = request.getContentDigest();
      if (digest.equals(instance.getDigestUtil().compute(request.getData()))) {
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
    } catch (IOException|StatusException ex) {
      responseObserver.onError(new StatusException(Status.fromThrowable(ex)));
      return;
    } catch (InterruptedException ex) {
      responseObserver.onError(new StatusException(Status.fromThrowable(ex)));
      Thread.currentThread().interrupt();
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

