package persistent.bazel.client;

import org.apache.commons.pool2.DestroyMode;
import persistent.common.CommonsPool;

/** Specializes CommmonsPool for PersistentWorker */
public class CommonsWorkerPool extends CommonsPool<WorkerKey, PersistentWorker> {
  private final WorkerIndex workerIndex;

  public CommonsWorkerPool(WorkerIndex workerIndex, WorkerSupervisor supervisor, int maxPerKey) {
    super(supervisor, maxPerKey);

    this.workerIndex = workerIndex;
  }

  @Override
  public void invalidateObject(WorkerKey key, PersistentWorker worker, DestroyMode destroyMode)
      throws Exception {
    super.invalidateObject(key, worker, destroyMode);

    workerIndex.removeWorker(worker);
  }
}
