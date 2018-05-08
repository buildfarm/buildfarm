package build.buildfarm.instance.shard;

import build.buildfarm.common.ShardBackplane;
import build.buildfarm.v1test.ShardDispatchedOperation;
import java.util.function.Predicate;

class DispatchedMonitor implements Runnable {
  private final ShardBackplane backplane;
  private final Predicate<String> requeueOperation;

  DispatchedMonitor(ShardBackplane backplane, Predicate<String> requeueOperation) {
    this.backplane = backplane;
    this.requeueOperation = requeueOperation;
  }

  @Override
  public synchronized void run() {
    while (true) {
      long now = System.currentTimeMillis(); /* FIXME sync */
      /* iterate over dispatched */
      for (ShardDispatchedOperation o : backplane.getDispatchedOperations()) {
        /* if now > dispatchedOperation.getExpiresAt() */
        if (now >= o.getRequeueAt()) {
          if (!requeueOperation.test(o.getName())) {
            backplane.deleteOperation(o.getName());
          }
          backplane.requeueDispatchedOperation(o);
        }
      }
      try {
        /* sleep? */
        this.wait(1, 0);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        break;
      }
    }
  }
}
