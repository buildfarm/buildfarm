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
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecutionPolicy;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.bazel.remote.execution.v2.ResultsCachePolicy;
import com.google.longrunning.Operation;
import com.google.protobuf.Duration;
import java.nio.file.Path;
import java.util.Map;

// FIXME MAKE THIS A PROTOBUF MESSAGE FOR EXECUTECONTEXT
// hm, can we encode maps???
final class OperationContext {
  final Operation operation;
  final Path execDir;
  final Map<Digest, Directory> directoriesIndex;
  final ExecuteOperationMetadata metadata;
  final Action action;
  final Command command;
  final Duration matchedIn;
  final Duration fetchedIn;
  final Duration executedIn;
  final ExecutionPolicy executionPolicy;
  final ResultsCachePolicy resultsCachePolicy;
  final RequestMetadata requestMetadata;

  private OperationContext(
      Operation operation,
      Path execDir,
      Map<Digest, Directory> directoriesIndex,
      ExecuteOperationMetadata metadata,
      Action action,
      Command command,
      Duration matchedIn,
      Duration fetchedIn,
      Duration executedIn,
      ExecutionPolicy executionPolicy,
      ResultsCachePolicy resultsCachePolicy,
      RequestMetadata requestMetadata) {
    this.operation = operation;
    this.execDir = execDir;
    this.directoriesIndex = directoriesIndex;
    this.metadata = metadata;
    this.action = action;
    this.command = command;
    this.matchedIn = matchedIn;
    this.fetchedIn = fetchedIn;
    this.executedIn = executedIn;
    this.executionPolicy = executionPolicy;
    this.resultsCachePolicy = resultsCachePolicy;
    this.requestMetadata = requestMetadata;
  }

  public static class Builder {
    private Operation operation;
    private Path execDir;
    private Map<Digest, Directory> directoriesIndex;
    private ExecuteOperationMetadata metadata;
    private Action action;
    private Command command;
    private Duration matchedIn;
    private Duration fetchedIn;
    private Duration executedIn;
    private ExecutionPolicy executionPolicy;
    private ResultsCachePolicy resultsCachePolicy;
    private RequestMetadata requestMetadata;

    private Builder(
        Operation operation,
        Path execDir,
        Map<Digest, Directory> directoriesIndex,
        ExecuteOperationMetadata metadata,
        Action action,
        Command command,
        Duration matchedIn,
        Duration fetchedIn,
        Duration executedIn,
        ExecutionPolicy executionPolicy,
        ResultsCachePolicy resultsCachePolicy,
        RequestMetadata requestMetadata) {
      this.operation = operation;
      this.execDir = execDir;
      this.directoriesIndex = directoriesIndex;
      this.metadata = metadata;
      this.action = action;
      this.command = command;
      this.matchedIn = matchedIn;
      this.fetchedIn = fetchedIn;
      this.executedIn = executedIn;
      this.executionPolicy = executionPolicy;
      this.resultsCachePolicy = resultsCachePolicy;
      this.requestMetadata = requestMetadata;
    }

    public Builder setOperation(Operation operation) {
      this.operation = operation;
      return this;
    }

    public Builder setExecDir(Path execDir) {
      this.execDir = execDir;
      return this;
    }

    public Builder setDirectoriesIndex(Map<Digest, Directory> directoriesIndex) {
      this.directoriesIndex = directoriesIndex;
      return this;
    }

    public Builder setMetadata(ExecuteOperationMetadata metadata) {
      this.metadata = metadata;
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

    public Builder setExecutionPolicy(ExecutionPolicy executionPolicy) {
      this.executionPolicy = executionPolicy;
      return this;
    }

    public Builder setResultsCachePolicy(ResultsCachePolicy resultsCachePolicy) {
      this.resultsCachePolicy = resultsCachePolicy;
      return this;
    }

    public Builder setRequestMetadata(RequestMetadata requestMetadata) {
      this.requestMetadata = requestMetadata;
      return this;
    }

    public OperationContext build() {
      return new OperationContext(
        operation,
        execDir,
        directoriesIndex,
        metadata,
        action,
        command,
        matchedIn,
        fetchedIn,
        executedIn,
        executionPolicy,
        resultsCachePolicy,
        requestMetadata);
    }
  }

  public static Builder newBuilder() {
    return new Builder(
        /* operation=*/ null,
        /* execDir=*/ null,
        /* directoriesIndex=*/ null,
        /* metadata=*/ null,
        /* action=*/ null,
        /* command=*/ null,
        /* matchedIn=*/ null,
        /* fetchedIn=*/ null,
        /* executedIn=*/ null,
        /* executionPolicy=*/ null,
        /* resultsCachePolicy=*/ null,
        /* requestMetadata=*/ null);
  }

  public Builder toBuilder() {
    return new Builder(
        operation,
        execDir,
        directoriesIndex,
        metadata,
        action,
        command,
        matchedIn,
        fetchedIn,
        executedIn,
        executionPolicy,
        resultsCachePolicy,
        requestMetadata);
  }
}
