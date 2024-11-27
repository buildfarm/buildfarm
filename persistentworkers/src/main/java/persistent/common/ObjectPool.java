package persistent.common;

public interface ObjectPool<K, V> {
  V obtain(K key) throws Exception;

  void release(K key, V obj);
}
