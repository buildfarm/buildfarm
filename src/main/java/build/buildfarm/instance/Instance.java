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

import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.devtools.remoteexecution.v1test.Action;
import com.google.devtools.remoteexecution.v1test.ActionResult;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.Directory;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata.Stage;
import com.google.devtools.remoteexecution.v1test.Platform;
import com.google.longrunning.Operation;
import com.google.protobuf.ByteString;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;

public interface Instance {
  String getName();

  DigestUtil getDigestUtil();

  void start();
  void stop();

  ActionResult getActionResult(ActionKey actionKey);
  void putActionResult(ActionKey actionKey, ActionResult actionResult);

  ListenableFuture<Iterable<Digest>> findMissingBlobs(Iterable<Digest> digests, ExecutorService service);

  String getBlobName(Digest blobDigest);
  void getBlob(Digest blobDigest, long offset, long limit, StreamObserver<ByteString> blobObserver);
  ChunkObserver getWriteBlobObserver(Digest blobDigest);
  ChunkObserver getWriteOperationStreamObserver(String operationStream);
  String getTree(
      Digest rootDigest,
      int pageSize,
      String pageToken,
      ImmutableList.Builder<Directory> directories,
      boolean acceptMissing) throws IOException, InterruptedException;
  CommittingOutputStream getStreamOutput(String name, long expectedSize);
  InputStream newStreamInput(String name, long offset) throws IOException, InterruptedException;

  ListenableFuture<Operation> execute(Action action, boolean skipCacheLookup);
  void match(Platform platform, MatchListener listener) throws InterruptedException;
  boolean putOperation(Operation operation) throws InterruptedException;
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

  // returns true if the operation will be handled in all cases through the
  // watcher.
  // The watcher returns true to indicate it is still able to process updates,
  // and returns false when it is complete and no longer wants updates
  // The watcher must not be tested again after it has returned false.
  boolean watchOperation(
      String operationName,
      boolean watchInitialState,
      Predicate<Operation> watcher);

  interface MatchListener {
    // start/end pair called for each wait period
    void onWaitStart();

    void onWaitEnd();

    // optional notification if distinct from operation fetch
    // returns false if this listener will not handle this match
    boolean onOperationName(String operationName);

    // returns false if this listener will not handle this match
    boolean onOperation(Operation operation);
  }

  public static class SimpleMatchListener implements MatchListener {
    private final Predicate<Operation> onMatch;

    public SimpleMatchListener(Predicate<Operation> onMatch) {
      this.onMatch = onMatch;
    }

    @Override public void onWaitStart() { }
    @Override public void onWaitEnd() { }
    @Override public boolean onOperationName(String operationName) { return true; }
    @Override public boolean onOperation(Operation operation) {
      return onMatch.test(operation);
    }
  }

  public static interface ChunkObserver extends StreamObserver<ByteString> {
    long getCommittedSize();

    void reset();

    ListenableFuture<Long> getCommittedFuture();
  }

  public static abstract class CommittingOutputStream extends OutputStream {
    public abstract ListenableFuture<Long> getCommittedFuture();
  }
}
