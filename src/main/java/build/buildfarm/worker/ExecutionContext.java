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
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.buildfarm.common.Poller;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.QueuedOperationMetadata;
import build.buildfarm.v1test.Tree;
import build.buildfarm.worker.resources.Claim;
import com.google.longrunning.Operation;
import java.nio.file.Path;

public final class ExecutionContext {
  final ExecuteResponse.Builder executeResponse;
  final Operation operation;
  final QueuedOperationMetadata.Builder metadata;
  final Poller poller;
  final Path execDir;
  final Action action;
  final Command command;
  final Tree tree;
  final QueueEntry queueEntry;
  public final Claim claim;

  private ExecutionContext(
      ExecuteResponse.Builder executeResponse,
      Operation operation,
      QueuedOperationMetadata.Builder metadata,
      Poller poller,
      Path execDir,
      Action action,
      Command command,
      Tree tree,
      QueueEntry queueEntry,
      Claim claim) {
    this.executeResponse = executeResponse;
    this.operation = operation;
    this.metadata = metadata;
    this.poller = poller;
    this.execDir = execDir;
    this.action = action;
    this.command = command;
    this.tree = tree;
    this.queueEntry = queueEntry;
    this.claim = claim;
  }

  public static final class Builder {
    private ExecuteResponse.Builder executeResponse;
    private Operation operation;
    private QueuedOperationMetadata.Builder metadata;
    private Poller poller;
    private Path execDir;
    private Action action;
    private Command command;
    private Tree tree;
    private QueueEntry queueEntry;
    private Claim claim;

    private Builder(
        ExecuteResponse.Builder executeResponse,
        Operation operation,
        QueuedOperationMetadata.Builder metadata,
        Poller poller,
        Path execDir,
        Action action,
        Command command,
        Tree tree,
        QueueEntry queueEntry,
        Claim claim) {
      this.executeResponse = executeResponse;
      this.operation = operation;
      this.metadata = metadata;
      this.poller = poller;
      this.execDir = execDir;
      this.action = action;
      this.command = command;
      this.tree = tree;
      this.queueEntry = queueEntry;
      this.claim = claim;
    }

    public Builder setOperation(Operation operation) {
      this.operation = operation;
      return this;
    }

    public Builder setMetadata(QueuedOperationMetadata.Builder metadata) {
      this.metadata = metadata;
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

    public Builder setTree(Tree tree) {
      this.tree = tree;
      return this;
    }

    public Builder setQueueEntry(QueueEntry queueEntry) {
      this.queueEntry = queueEntry;
      return this;
    }

    public Builder setClaim(Claim claim) {
      this.claim = claim;
      return this;
    }

    public ExecutionContext build() {
      return new ExecutionContext(
          executeResponse,
          operation,
          metadata,
          poller,
          execDir,
          action,
          command,
          tree,
          queueEntry,
          claim);
    }
  }

  public static Builder newBuilder() {
    return new Builder(
        /* executeResponse= */ ExecuteResponse.newBuilder(),
        /* operation= */ null,
        /* metadata= */ QueuedOperationMetadata.newBuilder(),
        /* poller= */ null,
        /* execDir= */ null,
        /* action= */ null,
        /* command= */ null,
        /* tree= */ null,
        /* queueEntry= */ null,
        /* claim= */ null);
  }

  public Builder toBuilder() {
    return new Builder(
        executeResponse,
        operation,
        metadata,
        poller,
        execDir,
        action,
        command,
        tree,
        queueEntry,
        claim);
  }
}
