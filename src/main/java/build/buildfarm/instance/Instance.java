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
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.ExecutionPolicy;
import build.bazel.remote.execution.v2.ResultsCachePolicy;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata.Stage;
import build.bazel.remote.execution.v2.Platform;
import build.bazel.remote.execution.v2.ServerCapabilities;
import com.google.longrunning.Operation;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.function.Predicate;

public interface Instance {
  String getName();

  DigestUtil getDigestUtil();

  ActionResult getActionResult(ActionKey actionKey);
  void putActionResult(ActionKey actionKey, ActionResult actionResult) throws InterruptedException;

  Iterable<Digest> findMissingBlobs(Iterable<Digest> digests);

  Iterable<Digest> putAllBlobs(Iterable<ByteString> blobs)
      throws IOException, IllegalArgumentException, InterruptedException;

  String getBlobName(Digest blobDigest);
  ByteString getBlob(Digest blobDigest);
  ByteString getBlob(Digest blobDigest, long offset, long limit);
  Digest putBlob(ByteString blob)
      throws IOException, IllegalArgumentException, InterruptedException;
  String getTree(
      Digest rootDigest,
      int pageSize,
      String pageToken,
      ImmutableList.Builder<Directory> directories); 
  OutputStream getStreamOutput(String name);
  InputStream newStreamInput(String name);

  void execute(
      Digest actionDigest,
      boolean skipCacheLookup,
      ExecutionPolicy executionPolicy,
      ResultsCachePolicy resultsCachePolicy,
      Predicate<Operation> onOperation) throws InterruptedException;
  void match(Platform platform, boolean requeueOnFailure, Predicate<Operation> onMatch) throws InterruptedException;
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
      Predicate<Operation> watcher);

  ServerCapabilities getCapabilities();
}
