package persistent.common;

import java.io.IOException;
import persistent.common.CtxAround.Id;

/**
 * Manages worker lifetimes and acts as the mediator between executors and workers. It also manages
 * pre-work initialization and post-work cleanup.
 *
 * @param <K> worker key type
 * @param <I> request type
 * @param <O> work response type
 * @param <W> worker type
 * @param <CI> request with extra context/info
 * @param <CO> response with extra context/info
 * @param <P> pool type
 */
public abstract class Coordinator<
    K,
    I,
    O,
    W extends Worker<I, O>,
    CI extends CtxAround<I>,
    CO extends CtxAround<O>,
    P extends ObjectPool<K, W>> {
  protected final P workerPool;

  public Coordinator(P workerPool) {
    this.workerPool = workerPool;
  }

  public CO runRequest(K workerKey, CI reqWithCtx) throws Exception {
    W worker = workerPool.obtain(workerKey);

    I request = preWorkInit(workerKey, reqWithCtx, worker);
    O workResponse = worker.doWork(request);
    CO responseAfterCLeanup = postWorkCleanup(workResponse, worker, reqWithCtx);

    workerPool.release(workerKey, worker);
    return responseAfterCLeanup;
  }

  public abstract I preWorkInit(K workerKey, CI request, W worker) throws IOException;

  public abstract CO postWorkCleanup(O response, W worker, CI request) throws IOException;

  public static <K, I, O, W extends Worker<I, O>> SimpleCoordinator<K, I, O, W> simple(
      ObjectPool<K, W> workerPool) {
    return new SimpleCoordinator<>(workerPool);
  }

  /**
   * A Coordinator which doesn't have any extra metadata for the request and response types Its pool
   * type is also filled in as an ObjectPool interface
   */
  public static class SimpleCoordinator<K, I, O, W extends Worker<I, O>>
      extends Coordinator<K, I, O, W, Id<I>, Id<O>, ObjectPool<K, W>> {
    public SimpleCoordinator(ObjectPool<K, W> workerPool) {
      super(workerPool);
    }

    @Override
    public I preWorkInit(K workerKey, Id<I> request, W worker) {
      return request.get();
    }

    @Override
    public Id<O> postWorkCleanup(O response, W worker, Id<I> request) {
      return Id.of(response);
    }
  }
}
