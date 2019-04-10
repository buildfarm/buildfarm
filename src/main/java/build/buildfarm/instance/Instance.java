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

package build.buildfarm.instance;

import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.BatchReadBlobsResponse.Response;
import build.bazel.remote.execution.v2.BatchUpdateBlobsResponse;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.ExecutionPolicy;
import build.bazel.remote.execution.v2.ResultsCachePolicy;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata.Stage;
import build.bazel.remote.execution.v2.Platform;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.bazel.remote.execution.v2.ServerCapabilities;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.Watcher;
import build.buildfarm.common.Write;
import build.buildfarm.v1test.QueueEntry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.longrunning.Operation;
import com.google.protobuf.ByteString;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;

public interface Instance {
  String getName();

  DigestUtil getDigestUtil();

  void start();
  void stop() throws InterruptedException;

  ActionResult getActionResult(ActionKey actionKey);
  void putActionResult(ActionKey actionKey, ActionResult actionResult) throws InterruptedException;

  ListenableFuture<Iterable<Digest>> findMissingBlobs(Iterable<Digest> digests, Executor executor);
  boolean containsBlob(Digest digest);

  String getBlobName(Digest blobDigest);
  void getBlob(Digest blobDigest, long offset, long limit, StreamObserver<ByteString> blobObserver);
  InputStream newBlobInput(Digest digest, long offset) throws IOException;
  ListenableFuture<Iterable<Response>> getAllBlobsFuture(Iterable<Digest> digests);
  String getTree(
      Digest rootDigest,
      int pageSize,
      String pageToken,
      ImmutableList.Builder<Directory> directories)
      throws IOException, InterruptedException;

  Write getBlobWrite(Digest digest, UUID uuid);
  Iterable<Digest> putAllBlobs(Iterable<ByteString> blobs)
      throws IOException, IllegalArgumentException, InterruptedException;

  Write getOperationStreamWrite(String name);
  InputStream newOperationStreamInput(String name, long offset) throws IOException;

  ListenableFuture<Void> execute(
      Digest actionDigest,
      boolean skipCacheLookup,
      ExecutionPolicy executionPolicy,
      ResultsCachePolicy resultsCachePolicy,
      RequestMetadata requestMetadata,
      Watcher operationObserver) throws InterruptedException;
  void match(Platform platform, MatchListener listener) throws InterruptedException;
  boolean putOperation(Operation operation) throws InterruptedException;
  boolean putAndValidateOperation(Operation operation) throws InterruptedException;
  boolean pollOperation(String operationName, Stage stage);
  // returns nextPageToken suitable for list restart
  String listOperations(
      int pageSize,
      String pageToken,
      String filter,
      ImmutableList.Builder<Operation> operations);
  Operation getOperation(String name);
  void cancelOperation(String name) throws InterruptedException;
  void deleteOperation(String name);

  ListenableFuture<Void> watchOperation(
      String operationName,
      Watcher watcher);

  ServerCapabilities getCapabilities();

  interface MatchListener {
    // start/end pair called for each wait period
    void onWaitStart();

    void onWaitEnd();

    // returns false if this listener will not handle this match
    boolean onEntry(@Nullable QueueEntry queueEntry) throws InterruptedException;
  }

  public static interface ChunkObserver extends StreamObserver<ByteString> {
    long getCommittedSize();

    void reset();

    ListenableFuture<Long> getCommittedFuture();
  }

  public static abstract class CommittingOutputStream extends OutputStream {
    public abstract ListenableFuture<Long> getCommittedFuture();
  }

  public static class PutAllBlobsException extends RuntimeException {
    private final List<BatchUpdateBlobsResponse.Response> failedResponses = Lists.newArrayList();

    public void addFailedResponse(BatchUpdateBlobsResponse.Response response) {
      failedResponses.add(response);
      addSuppressed(StatusProto.toStatusException(response.getStatus()));
    }

    public List<BatchUpdateBlobsResponse.Response> getFailedResponses() {
      return ImmutableList.copyOf(failedResponses);
    }
  }
}
