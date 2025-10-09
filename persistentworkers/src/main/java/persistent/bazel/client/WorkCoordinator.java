// Copyright 2023-2025 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package persistent.bazel.client;

import com.google.devtools.build.lib.worker.WorkerProtocol.WorkRequest;
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkResponse;
import persistent.common.Coordinator;
import persistent.common.CtxAround;
import persistent.common.ObjectPool;

/** Fills in/specializes Coordinator type parameters specifically for PersistentWorker usage */
public abstract class WorkCoordinator<
        I extends CtxAround<WorkRequest>,
        O extends CtxAround<WorkResponse>,
        P extends ObjectPool<WorkerKey, PersistentWorker>>
    extends Coordinator<WorkerKey, WorkRequest, WorkResponse, PersistentWorker, I, O, P> {
  public WorkCoordinator(P workerPool) {
    super(workerPool);
  }
}
