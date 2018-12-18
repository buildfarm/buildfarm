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

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata.Stage;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.Poller;
import build.buildfarm.common.function.InterruptingConsumer;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.Instance.MatchListener;
import build.buildfarm.instance.stub.ByteStreamUploader;
import build.buildfarm.v1test.CASInsertionPolicy;
import build.buildfarm.v1test.ExecutionPolicy;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.QueuedOperation;
import com.google.longrunning.Operation;
import com.google.protobuf.Duration;
import io.grpc.Deadline;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;

public interface WorkerContext {
  String getName();
  Poller createPoller(String name, QueueEntry queueEntry, Stage stage);
  void resumePoller(Poller poller, String name, QueueEntry queueEntry, Stage stage, Runnable onFailure, Deadline deadline);
  void match(MatchListener listener) throws InterruptedException;
  void requeue(Operation operation) throws InterruptedException;
  void deactivate(String operationName);
  void logInfo(String msg);
  CASInsertionPolicy getFileCasPolicy();
  CASInsertionPolicy getStdoutCasPolicy();
  CASInsertionPolicy getStderrCasPolicy();
  DigestUtil getDigestUtil();
  ExecutionPolicy getExecutionPolicy(String name);
  int getExecuteStageWidth();
  int getInputFetchStageWidth();
  int getTreePageSize();
  boolean hasDefaultActionTimeout();
  boolean hasMaximumActionTimeout();
  boolean getStreamStdout();
  boolean getStreamStderr();
  Duration getDefaultActionTimeout();
  Duration getMaximumActionTimeout();
  QueuedOperation getQueuedOperation(QueueEntry queueEntry) throws IOException, InterruptedException;
  Path createExecDir(String operationName, Iterable<Directory> directories, Action action, Command command) throws IOException, InterruptedException;
  void destroyExecDir(Path execDir) throws IOException, InterruptedException;
  void uploadOutputs(ActionResult.Builder resultBuilder, Path actionRoot, Iterable<String> outputFiles, Iterable<String> outputDirs) throws IOException, InterruptedException;
  boolean putOperation(Operation operation, Action Action) throws IOException, InterruptedException;
  OutputStream getStreamOutput(String name);
  void putActionResult(ActionKey actionKey, ActionResult actionResult) throws IOException, InterruptedException;
}
