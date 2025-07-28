package persistent.bazel.client;

import java.nio.file.attribute.UserPrincipal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import javax.annotation.Nullable;

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
