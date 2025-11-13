package build.buildfarm.common;

import java.time.Duration;
import java.util.function.Supplier;
import redis.clients.jedis.AbstractPipeline;
import redis.clients.jedis.resps.ScanResult;

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
  void visit(Visitor<E> visitor);

  void visitDequeue(Visitor<E> visitor);

  boolean removeFromDequeue(E e);

  void removeFromDequeue(AbstractPipeline pipeline, E e);

  ScanResult<E> scan(String cursor, int count, String match);
}
