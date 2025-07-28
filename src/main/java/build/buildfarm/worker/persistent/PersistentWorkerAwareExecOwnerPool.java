// Copyright 2023 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.worker.persistent;

import build.buildfarm.common.Claim.Stage;
import build.buildfarm.worker.resources.LocalResource;
import build.buildfarm.worker.resources.LocalResourceSet;
import java.nio.file.attribute.UserPrincipal;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import persistent.bazel.client.BasicWorkerKey;
import persistent.bazel.client.WorkerIndex;

public class PersistentWorkerAwareExecOwnerPool implements LocalResource {
  public static class Lease extends LocalResourceSet.PoolLease<String> {
    public final List<String> ownerNames;

    public Lease(Queue<String> pool, Stage stage, List<String> ownerNames) {
      super(pool, stage, ownerNames);

      this.ownerNames = ownerNames;
    }
  }

  private final Queue<String> pool;
  private final Set<String> takenForPersistentWorkers;
  private final Stage stage;
  private final WorkerIndex workerIndex;

  public PersistentWorkerAwareExecOwnerPool(
      WorkerIndex workerIndex, Iterable<String> execOwners, Stage stage) {
    this.workerIndex = workerIndex;
    this.pool = new ArrayDeque<>();

    for (String execOwner : execOwners) {
      pool.add(execOwner);
    }

    this.takenForPersistentWorkers = new HashSet<>();
    this.stage = stage;
  }

  @Override
  public int available() {
    return pool.size();
  }

  @Override
  public Optional<Lease> tryAcquire(int amount) {
    List<String> ownerNames = new ArrayList<>(amount);

    for (int i = 0; i < amount; i++) {
      String ownerName;

      while (true) {
        ownerName = pool.poll();

        if (ownerName == null || !takenForPersistentWorkers.contains(ownerName)) {
          break;
        }

        takenForPersistentWorkers.remove(ownerName);
      }

      ownerNames.add(ownerName);

      if (ownerName == null) {
        pool.addAll(ownerNames);

        return Optional.empty();
      }
    }

    return Optional.of(new Lease(pool, stage, ownerNames));
  }

  public Optional<Lease> tryAcquireForPersistentWorker(BasicWorkerKey workerKey) {
    UserPrincipal owner = workerIndex.getOwnerForIdleWorker(workerKey);

    if (owner == null) {
      // Ensure this stays in sync with the value of `EXEC_OWNER_PROPERTY`
      return tryAcquire(1);
    }

    String ownerName = owner.getName();

    takenForPersistentWorkers.add(ownerName);

    return Optional.of(
        new Lease(pool, stage, List.of(ownerName)) {
          @Override
          public void release() {
            super.release();

            takenForPersistentWorkers.remove(ownerName);
          }
        });
  }
}
