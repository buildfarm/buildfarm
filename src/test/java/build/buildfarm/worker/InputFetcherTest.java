// Copyright 2018 The Bazel Authors. All rights reserved.
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

import static build.buildfarm.common.Errors.VIOLATION_TYPE_MISSING;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.buildfarm.cas.cfc.PutDirectoryException;
import build.buildfarm.v1test.ExecuteEntry;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.QueuedOperation;
import build.buildfarm.worker.ExecDirException.ViolationException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.rpc.Code;
import com.google.rpc.DebugInfo;
import com.google.rpc.Help;
import com.google.rpc.LocalizedMessage;
import com.google.rpc.PreconditionFailure;
import com.google.rpc.RequestInfo;
import com.google.rpc.ResourceInfo;
import com.google.rpc.Status;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class InputFetcherTest {
  @Test
  public void onlyMissingFilesIsViolationMissingFailedPrecondition() throws Exception {
    PipelineStage error = mock(PipelineStage.class);
    Operation operation = Operation.newBuilder().setName("missing-inputs").build();
    ExecuteEntry executeEntry =
        ExecuteEntry.newBuilder().setOperationName(operation.getName()).build();
    QueueEntry queueEntry = QueueEntry.newBuilder().setExecuteEntry(executeEntry).build();
    ExecutionContext executionContext =
        ExecutionContext.newBuilder().setQueueEntry(queueEntry).setOperation(operation).build();
    Command command = Command.newBuilder().addArguments("/bin/false").build();
    QueuedOperation queuedOperation = QueuedOperation.newBuilder().setCommand(command).build();
    AtomicReference<Operation> failedOperationRef = new AtomicReference<>();
    WorkerContext workerContext =
        new StubWorkerContext() {
          @Override
          public QueuedOperation getQueuedOperation(QueueEntry queueEntry) {
            return queuedOperation;
          }

          @Override
          public boolean putOperation(Operation operation) {
            if (operation.getDone()) {
              return failedOperationRef.compareAndSet(null, operation);
            }
            return true;
          }

          @Override
          public Path createExecDir(
              String operationName,
              Map<Digest, Directory> directoriesIndex,
              Action action,
              Command command)
              throws IOException {
            Path root = Paths.get(operationName);
            throw new ExecDirException(
                Paths.get(operationName),
                ImmutableList.of(
                    new ViolationException(
                        Digest.getDefaultInstance(),
                        root.resolve("input"),
                        /* isExecutable= */ false,
                        new NoSuchFileException("input-digest")),
                    new PutDirectoryException(
                        root.resolve("dir"),
                        Digest.getDefaultInstance(),
                        ImmutableList.of(new NoSuchFileException("dir/input-digest")))));
          }

          @Override
          public int getInputFetchStageWidth() {
            return 1;
          }
        };
    InputFetchStage owner = new InputFetchStage(workerContext, /* output= */ null, error);
    InputFetcher inputFetcher = new InputFetcher(workerContext, executionContext, owner);
    inputFetcher.fetchPolled(/* stopwatch= */ null);
    Operation failedOperation = checkNotNull(failedOperationRef.get());
    verify(error, times(1)).put(any(ExecutionContext.class));
    ExecuteResponse executeResponse = failedOperation.getResponse().unpack(ExecuteResponse.class);
    Status status = executeResponse.getStatus();
    assertThat(status.getCode()).isEqualTo(Code.FAILED_PRECONDITION.getNumber());
    for (Any detail : status.getDetailsList()) {
      if (!(detail.is(DebugInfo.class)
          || detail.is(Help.class)
          || detail.is(LocalizedMessage.class)
          || detail.is(RequestInfo.class)
          || detail.is(ResourceInfo.class))) {
        assertThat(detail.is(PreconditionFailure.class)).isTrue();
        PreconditionFailure preconditionFailure = detail.unpack(PreconditionFailure.class);
        assertThat(preconditionFailure.getViolationsCount()).isGreaterThan(0);
        assertThat(
                Iterables.all(
                    preconditionFailure.getViolationsList(),
                    violation -> violation.getType().equals(VIOLATION_TYPE_MISSING)))
            .isTrue();
      }
    }
  }
}
