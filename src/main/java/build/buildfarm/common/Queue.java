package build.buildfarm.common;

import java.time.Duration;
import java.util.function.Supplier;
import redis.clients.jedis.AbstractPipeline;

public interface Queue<E> {
  // java.util.BlockingQueue-ish
  E take(Duration timeout) throws InterruptedException;

  // java.util.Queue
  E poll();

  boolean offer(E e);

  // our special variety
  boolean offer(E e, double priority);

  // java.util.Collection
  long size();

  Supplier<Long> size(AbstractPipeline pipeline);

  // maybe switch to iterator?
  void visit(StringVisitor visitor);

  void visitDequeue(StringVisitor visitor);

  boolean removeFromDequeue(E e);
}
