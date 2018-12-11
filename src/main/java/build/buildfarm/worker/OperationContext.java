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
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.buildfarm.v1test.QueueEntry;
import com.google.longrunning.Operation;
import com.google.protobuf.Duration;
import java.nio.file.Path;

// FIXME MAKE THIS A PROTOBUF MESSAGE FOR EXECUTECONTEXT
// hm, can we encode maps???
final class OperationContext {
  final Operation operation;
  final Path execDir;
  final Action action;
  final Command command;
  final Duration matchedIn;
  final Duration fetchedIn;
  final Duration executedIn;
  final QueueEntry queueEntry;

  private OperationContext(
      Operation operation,
      Path execDir,
      Action action,
      Command command,
      Duration matchedIn,
      Duration fetchedIn,
      Duration executedIn,
      QueueEntry queueEntry) {
    this.operation = operation;
    this.execDir = execDir;
    this.action = action;
    this.command = command;
    this.matchedIn = matchedIn;
    this.fetchedIn = fetchedIn;
    this.executedIn = executedIn;
    this.queueEntry = queueEntry;
  }

  public static class Builder {
    private Operation operation;
    private Path execDir;
    private Action action;
    private Command command;
    private Duration matchedIn;
    private Duration fetchedIn;
    private Duration executedIn;
    private QueueEntry queueEntry;

    private Builder(
        Operation operation,
        Path execDir,
        Action action,
        Command command,
        Duration matchedIn,
        Duration fetchedIn,
        Duration executedIn,
        QueueEntry queueEntry) {
      this.operation = operation;
      this.execDir = execDir;
      this.action = action;
      this.command = command;
      this.matchedIn = matchedIn;
      this.fetchedIn = fetchedIn;
      this.executedIn = executedIn;
      this.queueEntry = queueEntry;
    }

    public Builder setOperation(Operation operation) {
      this.operation = operation;
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

    public Builder setMatchedIn(Duration matchedIn) {
      this.matchedIn = matchedIn;
      return this;
    }

    public Builder setFetchedIn(Duration fetchedIn) {
      this.fetchedIn = fetchedIn;
      return this;
    }

    public Builder setExecutedIn(Duration executedIn) {
      this.executedIn = executedIn;
      return this;
    }

    public Builder setQueueEntry(QueueEntry queueEntry) {
      this.queueEntry = queueEntry;
      return this;
    }

    public OperationContext build() {
      return new OperationContext(
        operation,
        execDir,
        action,
        command,
        matchedIn,
        fetchedIn,
        executedIn,
        queueEntry);
    }
  }

  public static Builder newBuilder() {
    return new Builder(
        /* operation=*/ null,
        /* execDir=*/ null,
        /* action=*/ null,
        /* command=*/ null,
        /* matchedIn=*/ null,
        /* fetchedIn=*/ null,
        /* executedIn=*/ null,
        /* queueEntry=*/ null);
  }

  public Builder toBuilder() {
    return new Builder(
        operation,
        execDir,
        action,
        command,
        matchedIn,
        fetchedIn,
        executedIn,
        queueEntry);
  }
}
