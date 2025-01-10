package persistent.common;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;

// ObjectPool which uses Apache Commons
public abstract class CommonsObjPool<K, V> extends GenericKeyedObjectPool<K, V>
    implements ObjectPool<K, V> {
  public CommonsObjPool(
      KeyedPooledObjectFactory<K, V> factory, GenericKeyedObjectPoolConfig<V> config) {
    super(factory, config);
  }

  public V obtain(K key) throws Exception {
    return this.borrowObject(key);
  }

  public void release(K key, V obj) {
    this.returnObject(key, obj);
  }
}
