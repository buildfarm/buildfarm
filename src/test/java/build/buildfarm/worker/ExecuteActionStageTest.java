// Copyright 2018 The Buildfarm Authors. All rights reserved.
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import build.buildfarm.v1test.ExecuteEntry;
import build.buildfarm.v1test.QueueEntry;
import java.nio.file.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ExecuteActionStageTest {
  @Test
  public void errorPathDestroysExecDir() throws Exception {
    WorkerContext context = mock(WorkerContext.class);
    when(context.getExecuteStageWidth()).thenReturn(1);
    PipelineStage error = mock(PipelineStage.class);

    QueueEntry errorEntry =
        QueueEntry.newBuilder()
            .setExecuteEntry(ExecuteEntry.newBuilder().setOperationName("error"))
            .build();
    ExecutionContext errorContext =
        ExecutionContext.newBuilder()
            .setQueueEntry(errorEntry)
            .setExecDir(Path.of("error-operation-path"))
            .build();

    PipelineStage executeActionStage = new ExecuteActionStage(context, /* output= */ null, error);
    executeActionStage.error().put(errorContext);
    verify(context, times(1)).destroyExecDir(errorContext.execDir);
    verify(error, times(1)).put(errorContext);
  }
}
