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
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.lang.String.format;
import static java.util.logging.Level.SEVERE;

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
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.Instance.ChunkObserver;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.logging.Logger;

public class ContentAddressableStorageService extends ContentAddressableStorageGrpc.ContentAddressableStorageImplBase {
  private static final Logger logger = Logger.getLogger(ContentAddressableStorageService.class.getName());

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

    FindMissingBlobsResponse.Builder builder = FindMissingBlobsResponse.newBuilder();
    addCallback(
        transform(
            instance.findMissingBlobs(request.getBlobDigestsList(), newDirectExecutorService()),
            builder::addAllMissingBlobDigests),
        new FutureCallback<FindMissingBlobsResponse.Builder>() {
          @Override
          public void onSuccess(FindMissingBlobsResponse.Builder builder) {
            try {
              responseObserver.onNext(builder.build());
              responseObserver.onCompleted();
              logger.info(format("FindMissingBlobs(%s) for %d blobs", instance.getName(), request.getBlobDigestsList().size()));
            } catch (Throwable t) {
              onFailure(t);
            }
          }

          @Override
          public void onFailure(Throwable t) {
            Status status = Status.fromThrowable(t);
            if (status.getCode() != Code.CANCELLED) {
              logger.log(SEVERE, format("findMissingBlobs(%s): %s: %d", instance.getName(), request.getInstanceName(), request.getBlobDigestsCount()), t);
              responseObserver.onError(t);
            }
          }
        });
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
                .setDigest(digest)
                .setStatus(statusForCode(code))
                .build();
          }
        });
  }

  private static Iterable<ListenableFuture<Response>> putAllBlobs(Instance instance, Iterable<Request> requests) {
    ImmutableList.Builder<ListenableFuture<Response>> responses = new ImmutableList.Builder<>();
    for (Request request : requests) {
      Digest digest = request.getDigest();
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

  private void getInstanceTree(
      Instance instance,
      Digest rootDigest,
      String pageToken,
      int pageSize,
      StreamObserver<GetTreeResponse> responseObserver) {
    do {
      ImmutableList.Builder<Directory> directories = new ImmutableList.Builder<>();
      String nextPageToken = instance.getTree(
          rootDigest, pageSize, pageToken, directories);

      responseObserver.onNext(GetTreeResponse.newBuilder()
          .addAllDirectories(directories.build())
          .setNextPageToken(nextPageToken)
          .build());
      pageToken = nextPageToken;
    } while (!pageToken.isEmpty());
    responseObserver.onCompleted();
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

    getInstanceTree(
        instance,
        request.getRootDigest(),
        request.getPageToken(),
        pageSize,
        responseObserver);
  }
}
