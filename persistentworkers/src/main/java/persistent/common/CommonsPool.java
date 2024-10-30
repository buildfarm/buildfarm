package persistent.common;

import com.google.common.base.Throwables;
import java.io.IOException;
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;

/**
 * Pool based on Apache Commons, ripped from Bazel as usual
 *
 * @param <K>
 * @param <V>
 */
public class CommonsPool<K, V> extends CommonsObjPool<K, V> {

  public CommonsPool(BaseKeyedPooledObjectFactory<K, V> factory, int maxPerKey) {
    super(factory, makeConfig(maxPerKey));
  }

  @Override
  public V borrowObject(K key) throws IOException, InterruptedException {
    try {
      return super.borrowObject(key);
    } catch (Throwable t) {
      Throwables.propagateIfPossible(t, IOException.class, InterruptedException.class);
      throw new RuntimeException("unexpected@<borrowObject>", t);
    }
  }

  @Override
  public void invalidateObject(K key, V obj) throws IOException, InterruptedException {
    try {
      super.invalidateObject(key, obj);
    } catch (Throwable t) {
      Throwables.propagateIfPossible(t, IOException.class, InterruptedException.class);
      throw new RuntimeException("unexpected@<invalidateObject>", t);
    }
  }

  static <V> GenericKeyedObjectPoolConfig<V> makeConfig(int max) {
    GenericKeyedObjectPoolConfig<V> config = new GenericKeyedObjectPoolConfig<>();

    // It's better to re-use a worker as often as possible and keep it hot, in order to profit
    // from JIT optimizations as much as possible.
    config.setLifo(true);

    // Keep a fixed number of workers running per key.
    config.setMaxIdlePerKey(max);
    config.setMaxTotalPerKey(max);
    config.setMinIdlePerKey(max);

    // Don't limit the total number of worker processes, as otherwise the pool might be full of
    // workers for one WorkerKey and can't accommodate a worker for another WorkerKey.
    config.setMaxTotal(-1);

    // Wait for a worker to become ready when a thread needs one.
    config.setBlockWhenExhausted(true);

    // Always test the liveliness of worker processes.
    config.setTestOnBorrow(true);
    config.setTestOnCreate(true);
    config.setTestOnReturn(true);

    // No eviction of idle workers.
    config.setTimeBetweenEvictionRunsMillis(-1);
    return config;
  }
}
