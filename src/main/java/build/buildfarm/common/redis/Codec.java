// Copyright 2025 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.common.redis;

import build.bazel.remote.execution.v2.ActionResult;
import build.buildfarm.v1test.DispatchedOperation;
import build.buildfarm.v1test.ExecuteEntry;
import build.buildfarm.v1test.OperationChange;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.ShardWorker;
import build.buildfarm.v1test.WorkerChange;
import com.google.longrunning.Operation;

public record Codec(
    StringTranslator<ActionResult> actionResult,
    StringTranslator<ExecuteEntry> executeEntry,
    StringTranslator<QueueEntry> queueEntry,
    StringTranslator<Operation> execution,
    StringTranslator<OperationChange> operationChange,
    StringTranslator<DispatchedOperation> dispatchedExecution,
    StringTranslator<ShardWorker> worker,
    StringTranslator<WorkerChange> workerChange) {}
