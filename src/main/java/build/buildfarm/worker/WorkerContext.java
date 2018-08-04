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
import build.buildfarm.instance.Instance.MatchListener;
import build.buildfarm.instance.stub.ByteStreamUploader;
import build.buildfarm.v1test.CASInsertionPolicy;
import com.google.devtools.remoteexecution.v1test.Action;
import com.google.devtools.remoteexecution.v1test.ActionResult;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.Directory;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata.Stage;
import com.google.longrunning.Operation;
import com.google.protobuf.Duration;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Map;

public interface WorkerContext {
  String getName();
  Poller createPoller(String name, String operationName, Stage stage);
  Poller createPoller(String name, String operationName, Stage stage, Runnable onFailure);
  void match(MatchListener listener) throws InterruptedException;
  void requeue(Operation operation) throws InterruptedException;
  void deactivate(Operation operation);
  void logInfo(String msg);
  CASInsertionPolicy getFileCasPolicy();
  CASInsertionPolicy getStdoutCasPolicy();
  CASInsertionPolicy getStderrCasPolicy();
  DigestUtil getDigestUtil();
  int getInlineContentLimit();
  int getExecuteStageWidth();
  int getTreePageSize();
  boolean hasDefaultActionTimeout();
  boolean hasMaximumActionTimeout();
  boolean getStreamStdout();
  boolean getStreamStderr();
  Duration getDefaultActionTimeout();
  Duration getMaximumActionTimeout();
  void createActionRoot(Path root, Map<Digest, Directory> directoriesIndex, Action action) throws IOException, InterruptedException;
  void destroyActionRoot(Path root) throws IOException, InterruptedException;
  Path getRoot();
  void removeDirectory(Path path) throws IOException;
  void uploadOutputs(ActionResult.Builder resultBuilder, Path actionRoot, Iterable<String> outputFiles, Iterable<String> outputDirs) throws IOException, InterruptedException;
  boolean putOperation(Operation operation, Action Action) throws IOException, InterruptedException;
  OutputStream getStreamOutput(String name);
  void putActionResult(ActionKey actionKey, ActionResult actionResult);
}
