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

package build.buildfarm.instance;

import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.BatchReadBlobsResponse.Response;
import build.bazel.remote.execution.v2.BatchUpdateBlobsRequest;
import build.bazel.remote.execution.v2.BatchUpdateBlobsResponse;
import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.ExecutionPolicy;
import build.bazel.remote.execution.v2.ExecutionStage;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.bazel.remote.execution.v2.ResultsCachePolicy;
import build.bazel.remote.execution.v2.ServerCapabilities;
import build.buildfarm.common.CasIndexResults;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.EntryLimitException;
import build.buildfarm.common.Watcher;
import build.buildfarm.common.Write;
import build.buildfarm.v1test.BackplaneStatus;
import build.buildfarm.v1test.BatchWorkerProfilesResponse;
import build.buildfarm.v1test.GetClientStartTimeRequest;
import build.buildfarm.v1test.GetClientStartTimeResult;
import build.buildfarm.v1test.PipelineChange;
import build.buildfarm.v1test.PrepareWorkerForGracefulShutDownRequestResults;
import build.buildfarm.v1test.Tree;
import build.buildfarm.v1test.WorkerPipelineChangeResponse;
import build.buildfarm.v1test.WorkerProfileMessage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.longrunning.Operation;
import com.google.protobuf.ByteString;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.ServerCallStreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public interface Instance {
  String getName();

  void start(String publicName) throws IOException;

  void stop() throws InterruptedException;

  ListenableFuture<ActionResult> getActionResult(
      ActionKey actionKey, RequestMetadata requestMetadata);

  void putActionResult(ActionKey actionKey, ActionResult actionResult) throws InterruptedException;

  ListenableFuture<Iterable<Digest>> findMissingBlobs(
      Iterable<Digest> digests,
      DigestFunction.Value digestFunction,
      RequestMetadata requestMetadata);

  boolean containsBlob(
      build.buildfarm.v1test.Digest digest, Digest.Builder result, RequestMetadata requestMetadata)
      throws InterruptedException;

  String readResourceName(Compressor.Value compressor, build.buildfarm.v1test.Digest blobDigest);

  void getBlob(
      Compressor.Value compressor,
      build.buildfarm.v1test.Digest blobDigest,
      long offset,
      long count,
      ServerCallStreamObserver<ByteString> blobObserver,
      RequestMetadata requestMetadata);

  InputStream newBlobInput(
      Compressor.Value compressor,
      build.buildfarm.v1test.Digest digest,
      long offset,
      long deadlineAfter,
      TimeUnit deadlineAfterUnits,
      RequestMetadata requestMetadata)
      throws IOException;

  ListenableFuture<List<Response>> getAllBlobsFuture(
      Iterable<Digest> digests, DigestFunction.Value digestFunction);

  String getTree(
      build.buildfarm.v1test.Digest rootDigest, int pageSize, String pageToken, Tree.Builder tree);

  boolean isReadOnly();

  Write getBlobWrite(
      Compressor.Value compressor,
      build.buildfarm.v1test.Digest digest,
      UUID uuid,
      RequestMetadata requestMetadata)
      throws EntryLimitException;

  Iterable<Digest> putAllBlobs(
      Iterable<BatchUpdateBlobsRequest.Request> blobs,
      DigestFunction.Value digestFunction,
      RequestMetadata requestMetadata)
      throws IOException, IllegalArgumentException, InterruptedException;

  ListenableFuture<build.buildfarm.v1test.Digest> fetchBlob(
      Iterable<String> uris,
      Map<String, String> headers,
      build.buildfarm.v1test.Digest expectedDigest,
      RequestMetadata requestMetadata);

  Write getOperationStreamWrite(String name);

  InputStream newOperationStreamInput(String name, long offset, RequestMetadata requestMetadata)
      throws IOException;

  ListenableFuture<Void> execute(
      build.buildfarm.v1test.Digest actionDigest,
      boolean skipCacheLookup,
      ExecutionPolicy executionPolicy,
      ResultsCachePolicy resultsCachePolicy,
      RequestMetadata requestMetadata,
      Watcher operationObserver)
      throws InterruptedException;

  BackplaneStatus backplaneStatus();

  boolean putOperation(Operation operation) throws InterruptedException;

  boolean pollOperation(String operationName, ExecutionStage.Value stage);

  String SENTINEL_PAGE_TOKEN = "";

  // returns nextPageToken suitable for list restart
  // SHOULD THESE BE THE SAME API with
  // {instance_name}/[correlatedInvocations|toolInvocations|executions]/{uuid}???
  // we've put too much into operation, it's just a moniker
  // these should be 'actionExecutions' or something similar
  String listOperations(
      String name, int pageSize, String pageToken, String filter, Consumer<Operation> onOperation)
      throws IOException;

  Operation getOperation(String name);

  void cancelOperation(String name) throws InterruptedException;

  void deleteOperation(String name);

  ListenableFuture<Void> watchExecution(UUID executionId, Watcher watcher);

  ServerCapabilities getCapabilities();

  ListenableFuture<WorkerProfileMessage> getWorkerProfile(String name);

  ListenableFuture<BatchWorkerProfilesResponse> batchWorkerProfiles(Iterable<String> names);

  PrepareWorkerForGracefulShutDownRequestResults shutDownWorkerGracefully(String name);

  ListenableFuture<WorkerPipelineChangeResponse> pipelineChange(
      String name, List<PipelineChange> changes);

  GetClientStartTimeResult getClientStartTime(GetClientStartTimeRequest request);

  CasIndexResults reindexCas();

  void deregisterWorker(String workerName);

  class PutAllBlobsException extends RuntimeException {
    private final List<BatchUpdateBlobsResponse.Response> failedResponses = Lists.newArrayList();

    public void addFailedResponse(BatchUpdateBlobsResponse.Response response) {
      failedResponses.add(response);
      addSuppressed(StatusProto.toStatusException(response.getStatus()));
    }

    public List<BatchUpdateBlobsResponse.Response> getFailedResponses() {
      return ImmutableList.copyOf(failedResponses);
    }
  }

  String bindExecutions(UUID executionId);

  String bindToolInvocations(UUID toolInvocationId);

  String bindCorrelatedInvocations(UUID correlatedInvocationsId);

  UUID unbindExecutions(String operationName);
}
