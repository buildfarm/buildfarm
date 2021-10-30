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
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

import build.bazel.remote.execution.v2.BatchReadBlobsRequest;
import build.bazel.remote.execution.v2.BatchReadBlobsResponse;
import build.bazel.remote.execution.v2.BatchUpdateBlobsRequest;
import build.bazel.remote.execution.v2.BatchUpdateBlobsRequest.Request;
import build.bazel.remote.execution.v2.BatchUpdateBlobsResponse;
import build.bazel.remote.execution.v2.BatchUpdateBlobsResponse.Response;
import build.bazel.remote.execution.v2.ContentAddressableStorageGrpc;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.FindMissingBlobsRequest;
import build.bazel.remote.execution.v2.FindMissingBlobsResponse;
import build.bazel.remote.execution.v2.GetTreeRequest;
import build.bazel.remote.execution.v2.GetTreeResponse;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.grpc.TracingMetadataUtils;
import build.buildfarm.instance.Instance;
import build.buildfarm.v1test.Tree;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.stub.StreamObserver;
import io.prometheus.client.Histogram;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ContentAddressableStorageService
    extends ContentAddressableStorageGrpc.ContentAddressableStorageImplBase {
  private static final Logger logger =
      Logger.getLogger(ContentAddressableStorageService.class.getName());
  private static final Histogram missingBlobs =
      Histogram.build().name("missing_blobs").help("Find missing blobs.").register();

  private final Instance instance;
  private final long writeDeadlineAfter;
  private final TimeUnit writeDeadlineAfterUnits;

  public ContentAddressableStorageService(
      Instance instance, long writeDeadlineAfter, TimeUnit writeDeadlineAfterUnits) {
    this.instance = instance;
    this.writeDeadlineAfter = writeDeadlineAfter;
    this.writeDeadlineAfterUnits = writeDeadlineAfterUnits;
  }

  String checkMessage(Digest digest, boolean found) {
    return format(" (%s, %sfound)", DigestUtil.toString(digest), found ? "" : "not ");
  }

  @Override
  public void findMissingBlobs(
      FindMissingBlobsRequest request, StreamObserver<FindMissingBlobsResponse> responseObserver) {
    instanceFindMissingBlobs(instance, request, responseObserver);
  }

  void instanceFindMissingBlobs(
      Instance instance,
      FindMissingBlobsRequest request,
      StreamObserver<FindMissingBlobsResponse> responseObserver) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    FindMissingBlobsResponse.Builder builder = FindMissingBlobsResponse.newBuilder();
    ListenableFuture<FindMissingBlobsResponse.Builder> responseFuture =
        transform(
            instance.findMissingBlobs(
                request.getBlobDigestsList(), TracingMetadataUtils.fromCurrentContext()),
            builder::addAllMissingBlobDigests,
            directExecutor());
    addCallback(
        responseFuture,
        new FutureCallback<FindMissingBlobsResponse.Builder>() {
          @Override
          public void onSuccess(FindMissingBlobsResponse.Builder builder) {
            try {
              FindMissingBlobsResponse response = builder.build();
              responseObserver.onNext(response);
              responseObserver.onCompleted();
              long elapsedMicros = stopwatch.elapsed(MICROSECONDS);
              missingBlobs.observe(request.getBlobDigestsList().size());
              logger.log(
                  Level.FINE,
                  "FindMissingBlobs("
                      + instance.getName()
                      + ") for "
                      + request.getBlobDigestsList().size()
                      + " blobs in "
                      + elapsedMicros / 1000.0);
            } catch (Throwable t) {
              onFailure(t);
            }
          }

          @SuppressWarnings("NullableProblems")
          @Override
          public void onFailure(Throwable t) {
            Status status = Status.fromThrowable(t);
            if (status.getCode() != Code.CANCELLED) {
              logger.log(
                  Level.SEVERE,
                  format(
                      "findMissingBlobs(%s): %d",
                      request.getInstanceName(), request.getBlobDigestsCount()),
                  t);
              responseObserver.onError(t);
            }
          }
        },
        directExecutor());
  }

  private static com.google.rpc.Status statusForCode(Code code) {
    return com.google.rpc.Status.newBuilder().setCode(code.value()).build();
  }

  private static ListenableFuture<Response> toResponseFuture(
      ListenableFuture<Code> codeFuture, Digest digest) {
    return transform(
        codeFuture,
        code -> Response.newBuilder().setDigest(digest).setStatus(statusForCode(code)).build(),
        directExecutor());
  }

  private static Iterable<ListenableFuture<Response>> putAllBlobs(
      Instance instance,
      Iterable<Request> requests,
      long writeDeadlineAfter,
      TimeUnit writeDeadlineAfterUnits) {
    ImmutableList.Builder<ListenableFuture<Response>> responses = new ImmutableList.Builder<>();
    for (Request request : requests) {
      Digest digest = request.getDigest();
      ListenableFuture<Digest> future =
          putBlobFuture(
              instance,
              digest,
              request.getData(),
              writeDeadlineAfter,
              writeDeadlineAfterUnits,
              TracingMetadataUtils.fromCurrentContext());
      responses.add(
          toResponseFuture(
              catching(
                  transform(future, (d) -> Code.OK, directExecutor()),
                  Throwable.class,
                  (e) -> Status.fromThrowable(e).getCode(),
                  directExecutor()),
              digest));
    }
    return responses.build();
  }

  @Override
  public void batchUpdateBlobs(
      BatchUpdateBlobsRequest batchRequest,
      StreamObserver<BatchUpdateBlobsResponse> responseObserver) {
    BatchUpdateBlobsResponse.Builder response = BatchUpdateBlobsResponse.newBuilder();
    ListenableFuture<BatchUpdateBlobsResponse> responseFuture =
        transform(
            allAsList(
                StreamSupport.stream(
                        putAllBlobs(
                                instance,
                                batchRequest.getRequestsList(),
                                writeDeadlineAfter,
                                writeDeadlineAfterUnits)
                            .spliterator(),
                        false)
                    .map((future) -> transform(future, response::addResponses, directExecutor()))
                    .collect(Collectors.toList())),
            (result) -> response.build(),
            directExecutor());

    addCallback(
        responseFuture,
        new FutureCallback<BatchUpdateBlobsResponse>() {
          @Override
          public void onSuccess(BatchUpdateBlobsResponse response) {
            responseObserver.onNext(response);
            responseObserver.onCompleted();
          }

          @SuppressWarnings("NullableProblems")
          @Override
          public void onFailure(Throwable t) {
            responseObserver.onError(t);
          }
        },
        directExecutor());
  }

  private void getInstanceTree(
      Instance instance,
      Digest rootDigest,
      String pageToken,
      int pageSize,
      StreamObserver<GetTreeResponse> responseObserver) {
    do {
      Tree.Builder builder = Tree.newBuilder().setRootDigest(rootDigest);
      String nextPageToken = instance.getTree(rootDigest, pageSize, pageToken, builder);
      Tree tree = builder.build();

      GetTreeResponse.Builder response =
          GetTreeResponse.newBuilder().setNextPageToken(nextPageToken);
      response.addAllDirectories(tree.getDirectoriesMap().values());
      responseObserver.onNext(response.build());
      pageToken = nextPageToken;
    } while (!pageToken.isEmpty());
    responseObserver.onCompleted();
  }

  void batchReadBlobs(
      Instance instance,
      BatchReadBlobsRequest batchRequest,
      StreamObserver<BatchReadBlobsResponse> responseObserver) {
    BatchReadBlobsResponse.Builder response = BatchReadBlobsResponse.newBuilder();
    addCallback(
        transform(
            instance.getAllBlobsFuture(batchRequest.getDigestsList()),
            (responses) -> response.addAllResponses(responses).build(),
            directExecutor()),
        new FutureCallback<BatchReadBlobsResponse>() {
          @Override
          public void onSuccess(BatchReadBlobsResponse response) {
            responseObserver.onNext(response);
            responseObserver.onCompleted();
          }

          @SuppressWarnings("NullableProblems")
          @Override
          public void onFailure(Throwable t) {
            responseObserver.onError(Status.fromThrowable(t).asException());
          }
        },
        directExecutor());
  }

  @Override
  public void batchReadBlobs(
      BatchReadBlobsRequest batchRequest, StreamObserver<BatchReadBlobsResponse> responseObserver) {
    batchReadBlobs(instance, batchRequest, responseObserver);
  }

  @Override
  public void getTree(GetTreeRequest request, StreamObserver<GetTreeResponse> responseObserver) {
    int pageSize = request.getPageSize();
    if (pageSize < 0) {
      responseObserver.onError(Status.INVALID_ARGUMENT.asException());
      return;
    }

    getInstanceTree(
        instance, request.getRootDigest(), request.getPageToken(), pageSize, responseObserver);
  }
}
