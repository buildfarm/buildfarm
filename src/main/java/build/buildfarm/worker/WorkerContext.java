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

package build.buildfarm.worker;

import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.ByteStreamUploader;
import build.buildfarm.v1test.CASInsertionPolicy;
import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata.Stage;
import com.google.longrunning.Operation;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.function.Predicate;

public interface WorkerContext {
  Poller createPoller(String name, String operationName, Stage stage);
  Poller createPoller(String name, String operationName, Stage stage, Runnable onFailure);
  void match(Predicate<Operation> onMatch) throws InterruptedException;
  CASInsertionPolicy getFileCasPolicy();
  CASInsertionPolicy getStdoutCasPolicy();
  CASInsertionPolicy getStderrCasPolicy();
  DigestUtil getDigestUtil();
  int getInlineContentLimit();
  int getExecuteStageWidth();
  int getTreePageSize();
  boolean getLinkInputDirectories();
  boolean hasDefaultActionTimeout();
  boolean hasMaximumActionTimeout();
  boolean getStreamStdout();
  boolean getStreamStderr();
  Duration getDefaultActionTimeout();
  Duration getMaximumActionTimeout();
  ByteStreamUploader getUploader();
  ByteString getBlob(Digest digest);
  void createActionRoot(Path root, Action action, Command command) throws IOException, InterruptedException;
  void destroyActionRoot(Path root) throws IOException;
  Path getRoot();
  void removeDirectory(Path path) throws IOException;
  boolean putOperation(Operation operation) throws InterruptedException;
  OutputStream getStreamOutput(String name);
  void putActionResult(ActionKey actionKey, ActionResult actionResult) throws InterruptedException;
}
