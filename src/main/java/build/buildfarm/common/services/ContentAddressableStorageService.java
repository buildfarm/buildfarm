// Copyright 2017 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.common.services;

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
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.FindMissingBlobsRequest;
import build.bazel.remote.execution.v2.FindMissingBlobsResponse;
import build.bazel.remote.execution.v2.GetTreeRequest;
import build.bazel.remote.execution.v2.GetTreeResponse;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.config.BuildfarmConfigs;
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
import io.prometheus.metrics.core.metrics.Histogram;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.extern.java.Log;

@Log
public class ContentAddressableStorageService
    extends ContentAddressableStorageGrpc.ContentAddressableStorageImplBase {
  private static final Histogram missingBlobs =
      Histogram.builder().name("missing_blobs").help("Find missing blobs.").register();

  private final Instance instance;
  private final long writeDeadlineAfter;

  private static BuildfarmConfigs configs = BuildfarmConfigs.getInstance();

  public ContentAddressableStorageService(Instance instance) {
    this.instance = instance;
    this.writeDeadlineAfter = configs.getServer().getCasWriteTimeout();
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
                request.getBlobDigestsList(),
                request.getDigestFunction(),
                TracingMetadataUtils.fromCurrentContext()),
            builder::addAllMissingBlobDigests,
            directExecutor());
    addCallback(
        responseFuture,
        new FutureCallback<>() {
          @Override
          public void onSuccess(FindMissingBlobsResponse.Builder builder) {
            try {
              FindMissingBlobsResponse response = builder.build();
              responseObserver.onNext(response);
              responseObserver.onCompleted();
              long elapsedMicros = stopwatch.elapsed(MICROSECONDS);
              missingBlobs.observe(request.getBlobDigestsList().size());
              log.log(
                  Level.FINER,
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
              log.log(
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
      DigestFunction.Value digestFunction,
      long writeDeadlineAfter,
      TimeUnit writeDeadlineAfterUnits) {
    ImmutableList.Builder<ListenableFuture<Response>> responses = new ImmutableList.Builder<>();
    for (Request request : requests) {
      build.buildfarm.v1test.Digest digest =
          DigestUtil.fromDigest(request.getDigest(), digestFunction);
      ListenableFuture<build.buildfarm.v1test.Digest> future =
          putBlobFuture(
              instance,
              request.getCompressor(),
              digest,
              request.getData(),
              writeDeadlineAfter,
              writeDeadlineAfterUnits,
              TracingMetadataUtils.fromCurrentContext());
      responses.add(
          toResponseFuture(
              catching(
                  transform(future, d -> Code.OK, directExecutor()),
                  Throwable.class,
                  (e) -> Status.fromThrowable(e).getCode(),
                  directExecutor()),
              DigestUtil.toDigest(digest)));
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
                                batchRequest.getDigestFunction(),
                                writeDeadlineAfter,
                                TimeUnit.SECONDS)
                            .spliterator(),
                        false)
                    .map((future) -> transform(future, response::addResponses, directExecutor()))
                    .collect(Collectors.toList())),
            (result) -> response.build(),
            directExecutor());

    addCallback(
        responseFuture,
        new FutureCallback<>() {
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
      build.buildfarm.v1test.Digest rootDigest,
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
            instance.getAllBlobsFuture(
                batchRequest.getDigestsList(), batchRequest.getDigestFunction()),
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

    build.buildfarm.v1test.Digest rootDigest =
        DigestUtil.fromDigest(request.getRootDigest(), request.getDigestFunction());
    if (rootDigest.getDigestFunction() == DigestFunction.Value.UNKNOWN) {
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription(
                  format(
                      "digest %s did not match any known types", DigestUtil.toString(rootDigest)))
              .asException());
      return;
    }

    getInstanceTree(instance, rootDigest, request.getPageToken(), pageSize, responseObserver);
  }
}
