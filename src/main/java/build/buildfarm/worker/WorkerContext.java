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

package build.buildfarm.worker;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.ExecutionStage;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.Poller;
import build.buildfarm.common.Write;
import build.buildfarm.common.config.ExecutionPolicy;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.QueuedOperation;
import build.buildfarm.v1test.WorkerExecutedMetadata;
import build.buildfarm.worker.resources.ResourceLimits;
import com.google.common.collect.ImmutableList;
import com.google.longrunning.Operation;
import com.google.protobuf.Duration;
import io.grpc.Deadline;
import io.grpc.StatusException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.attribute.UserPrincipal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;

public interface WorkerContext {
  interface IOResource extends AutoCloseable {
    @Override
    void close() throws IOException;

    boolean isReferenced();
  }

  String getName();

  boolean shouldErrorOperationOnRemainingResources();

  Poller createPoller(
      String name, QueueEntry queueEntry, ExecutionStage.Value stage, Executor executor);

  void resumePoller(
      Poller poller,
      String name,
      QueueEntry queueEntry,
      ExecutionStage.Value stage,
      Runnable onFailure,
      Deadline deadline,
      Executor executor);

  boolean inGracefulShutdown();

  void prepareForGracefulShutdown();

  void match(MatchListener listener) throws InterruptedException;

  List<ExecutionPolicy> getExecutionPolicies(String name);

  int getExecuteStageWidth();

  int getInputFetchStageWidth();

  int getInputFetchDeadline();

  int getReportResultStageWidth();

  boolean hasDefaultActionTimeout();

  boolean hasMaximumActionTimeout();

  boolean isStreamStdout();

  boolean isStreamStderr();

  Duration getDefaultActionTimeout();

  Duration getMaximumActionTimeout();

  QueuedOperation getQueuedOperation(QueueEntry queueEntry)
      throws IOException, InterruptedException;

  Path createExecDir(
      String operationName,
      Map<Digest, Directory> directoriesIndex,
      DigestFunction.Value digestFunction,
      Action action,
      Command command,
      @Nullable UserPrincipal owner,
      WorkerExecutedMetadata.Builder workerExecutedMetadata)
      throws IOException, InterruptedException;

  void destroyExecDir(Path execDir) throws IOException, InterruptedException;

  void uploadOutputs(
      build.buildfarm.v1test.Digest actionDigest,
      ActionResult.Builder resultBuilder,
      Path actionRoot,
      Command command)
      throws IOException, InterruptedException, StatusException;

  void unmergeExecution(ActionKey actionKey) throws IOException, InterruptedException;

  boolean putOperation(Operation operation) throws IOException, InterruptedException;

  void blocklistAction(String actionId) throws IOException, InterruptedException;

  void putActionResult(ActionKey actionKey, ActionResult actionResult)
      throws IOException, InterruptedException;

  Write getOperationStreamWrite(String name) throws IOException;

  long getStandardOutputLimit();

  long getStandardErrorLimit();

  void createExecutionLimits();

  void destroyExecutionLimits();

  /**
   * Apply cgroups limitation wrapper first, before any privilege-dropping wrappers. This is
   * required for cgroupsv2 where the wrapper needs to write to cgroup.procs as root.
   *
   * @param operationName the name of the operation
   * @param owner the owner to run as (may be null)
   * @param arguments the command arguments builder to prepend the wrapper to
   * @param command the command being executed
   * @return an IOResource for the cgroup, or null if cgroups are not enabled
   */
  @Nullable
  IOResource applyCgroupsLimitation(
      String operationName,
      @Nullable UserPrincipal owner,
      ImmutableList.Builder<String> arguments,
      Command command);

  /**
   * Apply remaining execution limits (linux-sandbox, as-nobody, etc.) AFTER cgroups wrapper.
   *
   * @param operationName the name of the operation
   * @param owner the owner to run as (may be null)
   * @param arguments the command arguments builder to prepend wrappers to
   * @param command the command being executed
   * @param workingDirectory the working directory for execution
   * @param cgroupResource the cgroup resource from applyCgroupsLimitation, may be null
   * @return an IOResource that manages the execution limits
   */
  IOResource limitExecution(
      String operationName,
      @Nullable UserPrincipal owner,
      ImmutableList.Builder<String> arguments,
      Command command,
      Path workingDirectory,
      @Nullable IOResource cgroupResource);

  int commandExecutionClaims(Command command);

  ResourceLimits commandExecutionSettings(Command command);
}
