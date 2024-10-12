package persistent.common;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

/**
 * A Supervisor is an ObjectFactory with default `wrap` and `destroyObject` overrides, and abstract
 * `create` and `validateObject` methods.
 *
 * <p>An aside: Why does an ObjectFactory do more than create the object?
 *
 * @param <K>
 * @param <V> must be a Destructable
 */
public abstract class CommonsSupervisor<K, V extends Destructable>
    extends BaseKeyedPooledObjectFactory<K, V> implements Supervisor<K, V, PooledObject<V>> {

  private static final Logger logger = Logger.getLogger(CommonsSupervisor.class.getName());

  public abstract V create(K k) throws Exception;

  public abstract boolean validateObject(K key, PooledObject<V> p);

  protected Logger getLogger() {
    return logger;
  }

  @Override
  public PooledObject<V> wrap(V v) {
    return new DefaultPooledObject<>(v);
  }

  @Override
  public void destroyObject(K key, PooledObject<V> p) {
    V obj = p.getObject();

    StringBuilder msgBuilder = new StringBuilder();
    msgBuilder.append("Supervisor.destroyObject() from key:\n");
    msgBuilder.append(key);
    msgBuilder.append("\nobj.toString():\n");
    msgBuilder.append(obj);
    msgBuilder.append("\nSupervisor.destroyObject() stackTrack:");
    for (StackTraceElement e : Thread.currentThread().getStackTrace()) {
      msgBuilder.append("\n\t");
      msgBuilder.append(e);
    }

    getLogger().log(Level.FINE, msgBuilder.toString());

    obj.destroy();
  }
}
