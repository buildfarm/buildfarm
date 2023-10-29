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
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.buildfarm.common.Poller;
import build.buildfarm.v1test.QueueEntry;
import com.google.longrunning.Operation;
import java.nio.file.Path;

final class OperationContext {
  final ExecuteResponse.Builder executeResponse;
  final Operation operation;
  final Poller poller;
  final Path execDir;
  final Action action;
  final Command command;
  final QueueEntry queueEntry;

  private OperationContext(
      ExecuteResponse.Builder executeResponse,
      Operation operation,
      Poller poller,
      Path execDir,
      Action action,
      Command command,
      QueueEntry queueEntry) {
    this.executeResponse = executeResponse;
    this.operation = operation;
    this.poller = poller;
    this.execDir = execDir;
    this.action = action;
    this.command = command;
    this.queueEntry = queueEntry;
  }

  public static class Builder {
    private ExecuteResponse.Builder executeResponse;
    private Operation operation;
    private Poller poller;
    private Path execDir;
    private Action action;
    private Command command;
    private QueueEntry queueEntry;

    private Builder(
        ExecuteResponse.Builder executeResponse,
        Operation operation,
        Poller poller,
        Path execDir,
        Action action,
        Command command,
        QueueEntry queueEntry) {
      this.executeResponse = executeResponse;
      this.operation = operation;
      this.poller = poller;
      this.execDir = execDir;
      this.action = action;
      this.command = command;
      this.queueEntry = queueEntry;
    }

    public Builder setOperation(Operation operation) {
      this.operation = operation;
      return this;
    }

    public Builder setPoller(Poller poller) {
      this.poller = poller;
      return this;
    }

    public Builder setExecDir(Path execDir) {
      this.execDir = execDir;
      return this;
    }

    public Builder setAction(Action action) {
      this.action = action;
      return this;
    }

    public Builder setCommand(Command command) {
      this.command = command;
      return this;
    }

    public Builder setQueueEntry(QueueEntry queueEntry) {
      this.queueEntry = queueEntry;
      return this;
    }

    public OperationContext build() {
      return new OperationContext(
          executeResponse, operation, poller, execDir, action, command, queueEntry);
    }
  }

  public static Builder newBuilder() {
    return new Builder(
        /* executeResponse=*/ ExecuteResponse.newBuilder(),
        /* operation=*/ null,
        /* poller=*/ null,
        /* execDir=*/ null,
        /* action=*/ null,
        /* command=*/ null,
        /* queueEntry=*/ null);
  }

  public Builder toBuilder() {
    return new Builder(executeResponse, operation, poller, execDir, action, command, queueEntry);
  }
}
