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

import static build.buildfarm.instance.Utils.putBlobFuture;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.Futures.catching;
import static com.google.common.util.concurrent.Futures.transform;

import build.buildfarm.instance.Instance;
import build.buildfarm.instance.Instance.ChunkObserver;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.devtools.remoteexecution.v1test.BatchUpdateBlobsRequest;
import com.google.devtools.remoteexecution.v1test.BatchUpdateBlobsResponse;
import com.google.devtools.remoteexecution.v1test.BatchUpdateBlobsResponse.Response;
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
import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;

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
    } catch (InstanceNotFoundException e) {
      responseObserver.onError(BuildFarmInstances.toStatusException(e));
      return;
    }

    responseObserver.onNext(FindMissingBlobsResponse.newBuilder()
        .addAllMissingBlobDigests(
            instance.findMissingBlobs(request.getBlobDigestsList()))
        .build());
    responseObserver.onCompleted();
  }

  private static com.google.rpc.Status statusForCode(Code code) {
    return com.google.rpc.Status.newBuilder()
        .setCode(code.value())
        .build();
  }

  private static ListenableFuture<Response> toResponseFuture(ListenableFuture<Code> codeFuture, Digest digest) {
    return transform(
        codeFuture,
        new Function<Code, Response>() {
          @Override
          public Response apply(Code code) {
            return Response.newBuilder()
                .setBlobDigest(digest)
                .setStatus(statusForCode(code))
                .build();
          }
        });
  }

  private static Iterable<ListenableFuture<Response>> putAllBlobs(Instance instance, Iterable<UpdateBlobRequest> requests) {
    ImmutableList.Builder<ListenableFuture<Response>> responses = new ImmutableList.Builder<>();
    for (UpdateBlobRequest request : requests) {
      Digest digest = request.getContentDigest();
      responses.add(
          toResponseFuture(
              catching(
                  transform(putBlobFuture(instance, digest, request.getData()), (d) -> Code.OK),
                  Throwable.class,
                  (e) -> Status.fromThrowable(e).getCode()),
              digest));
    }
    return responses.build();
  }

  @Override
  public void batchUpdateBlobs(
      BatchUpdateBlobsRequest batchRequest,
      StreamObserver<BatchUpdateBlobsResponse> responseObserver) {
    Instance instance;
    try {
      instance = instances.get(batchRequest.getInstanceName());
    } catch (InstanceNotFoundException e) {
      responseObserver.onError(BuildFarmInstances.toStatusException(e));
      return;
    }

    BatchUpdateBlobsResponse.Builder response = BatchUpdateBlobsResponse.newBuilder();
    ListenableFuture<BatchUpdateBlobsResponse> responseFuture = transform(
        allAsList(
            Iterables.transform(
                putAllBlobs(instance, batchRequest.getRequestsList()),
                (future) -> transform(future, response::addResponses))),
        (result) -> response.build());

    addCallback(responseFuture, new FutureCallback<BatchUpdateBlobsResponse>() {
      @Override
      public void onSuccess(BatchUpdateBlobsResponse response) {
        responseObserver.onNext(response);
        responseObserver.onCompleted();
      }

      @Override
      public void onFailure(Throwable t) {
        responseObserver.onError(t);
      }
    });
  }

  @Override
  public void getTree(
      GetTreeRequest request,
      StreamObserver<GetTreeResponse> responseObserver) {
    Instance instance;
    try {
      instance = instances.get(request.getInstanceName());
    } catch (InstanceNotFoundException e) {
      responseObserver.onError(BuildFarmInstances.toStatusException(e));
      return;
    }

    int pageSize = request.getPageSize();
    if (pageSize < 0) {
      responseObserver.onError(Status.INVALID_ARGUMENT.asException());
      return;
    }
    ImmutableList.Builder<Directory> directories = new ImmutableList.Builder<>();
    try {
      String nextPageToken = instance.getTree(
          request.getRootDigest(), pageSize, request.getPageToken(), directories, /*acceptMissing=*/ true);

      responseObserver.onNext(GetTreeResponse.newBuilder()
          .addAllDirectories(directories.build())
          .setNextPageToken(nextPageToken)
          .build());
      responseObserver.onCompleted();
    } catch (InterruptedException|IOException e) {
      responseObserver.onError(Status.fromThrowable(e).asException());
    }
  }
}

