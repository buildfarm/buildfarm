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

package build.buildfarm.worker.resources;

import build.buildfarm.common.Claim.Lease;
import build.buildfarm.common.Claim.Stage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.Semaphore;

/**
 * @class Local Resource Set
 * @brief A fixed amount of a specific resource.
 * @details We define limited resources as a counting semaphores whose configuration contains a name
 *     and a count representing a physical or logical group of units obtained by executors as a
 *     precondition to fulfill a long running operation. These units are released upon the
 *     operation's completion. The resource is requested by the action's platform properties. These
 *     resources are specific to the individual worker.
 */
public class LocalResourceSet {
  public record SemaphoreLease(Semaphore semaphore, Stage stage, int amount) implements Lease {
    @Override
    public int getAmount() {
      return amount;
    }

    @Override
    public Stage getStage() {
      return stage;
    }

    @Override
    public void release() {
      semaphore.release(amount);
    }
  }

  public record SemaphoreResource(Semaphore semaphore, Stage stage) implements LocalResource {
    @Override
    public int available() {
      return semaphore.availablePermits();
    }

    @Override
    public Optional<Lease> tryAcquire(int amount) {
      if (semaphore.tryAcquire(amount)) {
        return Optional.of(new SemaphoreLease(semaphore, stage, amount));
      }

      return Optional.empty();
    }
  }

  public record PoolLease(Queue<Object> pool, Stage stage, List<Object> claims) implements Lease {
    @Override
    public int getAmount() {
      return claims.size();
    }

    @Override
    public Stage getStage() {
      return stage;
    }

    @Override
    public void release() {
      pool.addAll(claims);
    }
  }

  public record PoolResource(Queue<Object> pool, Stage stage) implements LocalResource {
    @Override
    public int available() {
      return pool.size();
    }

    @Override
    public Optional<Lease> tryAcquire(int amount) {
      List<Object> claimedIds = new ArrayList<>(amount);

      for (int i = 0; i < amount; i++) {
        Object id = pool.poll();

        claimedIds.add(id);

        if (id == null) {
          pool.addAll(claimedIds);

          return Optional.empty();
        }
      }

      return Optional.of(new PoolLease(pool, stage, claimedIds));
    }
  }

  /**
   * @field resources
   * @brief A set containing resources organized by name.
   * @details Key is name, and value contains current usage amount.
   */
  public Map<String, LocalResource> resources = new HashMap<>();
}
