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
import com.google.devtools.remoteexecution.v1test.Action;
import com.google.devtools.remoteexecution.v1test.ActionResult;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.Directory;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata.Stage;
import com.google.devtools.remoteexecution.v1test.Platform;
import com.google.longrunning.Operation;
import com.google.protobuf.ByteString;
import io.grpc.StatusException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.function.Consumer;
import java.util.function.Predicate;

public interface Instance {
  String getName();

  DigestUtil getDigestUtil();

  void start();
  void stop();

  ActionResult getActionResult(ActionKey actionKey);
  void putActionResult(ActionKey actionKey, ActionResult actionResult);

  Iterable<Digest> findMissingBlobs(Iterable<Digest> digests);

  Iterable<Digest> putAllBlobs(Iterable<ByteString> blobs)
      throws IOException, InterruptedException, StatusException;

  String getBlobName(Digest blobDigest);
  ByteString getBlob(Digest blobDigest) throws InterruptedException, IOException;
  ByteString getBlob(Digest blobDigest, long offset, long limit) throws InterruptedException, IOException;
  Digest putBlob(ByteString blob)
      throws IOException, InterruptedException, StatusException;
  String getTree(
      Digest rootDigest,
      int pageSize,
      String pageToken,
      ImmutableList.Builder<Directory> directories,
      boolean acceptMissing) throws InterruptedException, IOException; 
  OutputStream getStreamOutput(String name);
  InputStream newStreamInput(String name) throws InterruptedException, IOException;

  void execute(
      Action action,
      boolean skipCacheLookup,
      int totalInputFileCount,
      long totalInputFileBytes,
      Consumer<Operation> onOperation) throws InterruptedException;
  void match(Platform platform, Predicate<Operation> onMatch) throws InterruptedException;
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

  boolean watchOperation(
      String operationName,
      boolean watchInitialState,
      Predicate<Operation> watcher);
}
