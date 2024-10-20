// Copyright 2019 The Buildfarm Authors. All rights reserved.
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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.Poller;
import build.buildfarm.v1test.Digest;
import build.buildfarm.v1test.ExecuteEntry;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.QueuedOperationMetadata;
import com.google.common.collect.Iterables;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.rpc.Code;
import com.google.rpc.Status;
import io.grpc.protobuf.StatusProto;
import java.nio.file.Paths;
import java.util.logging.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

@RunWith(JUnit4.class)
public class ReportResultStageTest {
  private final DigestUtil DIGEST_UTIL = new DigestUtil(HashFunction.SHA256);

  static class SingleOutputSink extends PipelineStage {
    ExecutionContext executionContext = null;

    public SingleOutputSink() {
      super("SingleOutputSink", /* workerContext= */ null, /* output= */ null, /* error= */ null);
    }

    @Override
    Logger getLogger() {
      return null;
    }

    @Override
    ExecutionContext take() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void put(ExecutionContext executionContext) {
      checkNotNull(executionContext);
      assertThat(this.executionContext).isNull();
      this.executionContext = executionContext;
      close();
    }

    ExecutionContext get() {
      checkNotNull(executionContext);
      return executionContext;
    }
  }

  @Test
  public void execDirDestroyedAfterComplete() throws Exception {
    WorkerContext context = mock(WorkerContext.class);
    SingleOutputSink output = new SingleOutputSink();

    Operation reportedOperation =
        Operation.newBuilder()
            .setName("reported")
            .setMetadata(Any.pack(QueuedOperationMetadata.getDefaultInstance()))
            .build();
    QueueEntry reportedEntry =
        QueueEntry.newBuilder()
            .setExecuteEntry(
                ExecuteEntry.newBuilder().setOperationName(reportedOperation.getName()))
            .build();
    ExecutionContext reportedContext =
        ExecutionContext.newBuilder()
            .setCommand(Command.getDefaultInstance())
            .setAction(Action.newBuilder().setDoNotCache(true).build())
            .setOperation(reportedOperation)
            .setQueueEntry(reportedEntry)
            .setExecDir(Paths.get("reported-operation-path"))
            .setPoller(mock(Poller.class))
            .build();
    when(context.getReportResultStageWidth()).thenReturn(1);
    when(context.putOperation(any(Operation.class))).thenReturn(true);

    PipelineStage reportResultStage = new ReportResultStage(context, output, /* error= */ null);
    reportResultStage.claim(reportedContext);
    reportResultStage.put(reportedContext);
    reportResultStage.run();
    verify(context, times(1)).destroyExecDir(reportedContext.execDir);
    verify(context, times(1)).getReportResultStageWidth();
    ArgumentCaptor<Operation> operationCaptor = ArgumentCaptor.forClass(Operation.class);
    verify(context, times(2)).putOperation(operationCaptor.capture());
    Operation completeOperation = Iterables.getLast(operationCaptor.getAllValues());
    assertThat(output.get().operation).isEqualTo(completeOperation);
  }

  @Test
  public void operationErrorOnStatusException() throws Exception {
    WorkerContext context = mock(WorkerContext.class);
    SingleOutputSink output = new SingleOutputSink();

    Operation erroringOperation =
        Operation.newBuilder()
            .setName("erroring")
            .setMetadata(Any.pack(QueuedOperationMetadata.getDefaultInstance()))
            .build();
    Action action = Action.getDefaultInstance();
    Digest actionDigest = DIGEST_UTIL.compute(action);
    QueueEntry erroringEntry =
        QueueEntry.newBuilder()
            .setExecuteEntry(
                ExecuteEntry.newBuilder()
                    .setOperationName(erroringOperation.getName())
                    .setActionDigest(actionDigest))
            .build();
    ExecutionContext erroringContext =
        ExecutionContext.newBuilder()
            .setCommand(Command.getDefaultInstance())
            .setAction(action)
            .setOperation(erroringOperation)
            .setQueueEntry(erroringEntry)
            .setExecDir(Paths.get("erroring-operation-path"))
            .setPoller(mock(Poller.class))
            .build();
    when(context.getReportResultStageWidth()).thenReturn(1);
    when(context.putOperation(any(Operation.class))).thenReturn(true);
    Status erroredStatus =
        Status.newBuilder().setCode(Code.FAILED_PRECONDITION.getNumber()).build();
    doThrow(StatusProto.toStatusException(erroredStatus))
        .when(context)
        .uploadOutputs(
            eq(actionDigest),
            any(ActionResult.Builder.class),
            eq(erroringContext.execDir),
            eq(Command.getDefaultInstance()));

    PipelineStage reportResultStage = new ReportResultStage(context, output, /* error= */ null);
    reportResultStage.claim(erroringContext);
    reportResultStage.put(erroringContext);
    reportResultStage.run();
    verify(context, times(1)).getReportResultStageWidth();
    verify(context, times(1)).destroyExecDir(erroringContext.execDir);
    ArgumentCaptor<Operation> operationCaptor = ArgumentCaptor.forClass(Operation.class);
    verify(context, times(2)).putOperation(operationCaptor.capture());
    Operation erroredOperation = Iterables.getLast(operationCaptor.getAllValues());
    assertThat(output.get().operation).isEqualTo(erroredOperation);
    verify(context, times(1))
        .uploadOutputs(
            eq(DIGEST_UTIL.compute(erroringContext.action)),
            any(ActionResult.Builder.class),
            eq(erroringContext.execDir),
            eq(Command.getDefaultInstance()));
    assertThat(erroredOperation.getResponse().unpack(ExecuteResponse.class).getStatus())
        .isEqualTo(erroredStatus);
    verify(context, never()).putActionResult(any(ActionKey.class), any(ActionResult.class));
  }
}
