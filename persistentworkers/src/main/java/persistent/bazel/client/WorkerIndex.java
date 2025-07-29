package persistent.bazel.client;

import java.nio.file.attribute.UserPrincipal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * This class augments the {@link CommonsWorkerPool worker pool} with a persistent worker map whose
 * key is the worker's {@link BasicWorkerKey}, not its {@link WorkerKey}.
 *
 * <p>This allows us to, given a {@link BasicWorkerKey}, preemptively select an exec owner for which
 * we already have an alive worker with that key. That way, we avoid creating too many workers,
 * which would happen by virtue of the fact that {@link WorkerKey} is the key of the {@link
 * CommonsWorkerPool worker pool}.
 *
 * <p>A worker is inserted into the worker index when it's created and removed when it's destroyed.
 */
public class WorkerIndex {
  private final Map<BasicWorkerKey, Set<PersistentWorker>> workerIndex;

  public WorkerIndex() {
    workerIndex = new HashMap<>();
  }

  public @Nullable UserPrincipal getOwnerForIdleWorker(BasicWorkerKey workerKey) {
    Set<PersistentWorker> idleWorkers = workerIndex.get(workerKey);

    if (idleWorkers == null) {
      return null;
    }

    try {
      return idleWorkers.iterator().next().getKey().getOwner();
    } catch (NoSuchElementException exception) {
      return null;
    }
  }

  public void registerWorker(PersistentWorker worker) {
    workerIndex
        .computeIfAbsent(worker.getKey().getBasicWorkerKey(), key -> new HashSet<>())
        .add(worker);
  }

  public void removeWorker(PersistentWorker worker) {
    Set<PersistentWorker> workersForKey = workerIndex.get(worker.getKey().getBasicWorkerKey());

    if (workersForKey != null) {
      workersForKey.remove(worker);
    }
  }
}
