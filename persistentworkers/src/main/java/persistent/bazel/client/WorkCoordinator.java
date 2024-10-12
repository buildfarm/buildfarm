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
