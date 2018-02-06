package build.buildfarm.instance.shard;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

class ConcurrentLRUCache<K, V> extends LinkedHashMap<K, V> implements ConcurrentMap<K, V> {
  private int cacheSize;

  public ConcurrentLRUCache(int cacheSize) {
    super(16, 0.75f, true);
    this.cacheSize = cacheSize;
  }

  @Override
  protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
    return size() >= cacheSize;
  }

  @Override
  synchronized public V get(Object key) {
    return super.get(key);
  }

  @Override
  synchronized public V put(K key, V value) {
    return super.put(key, value);
  }
}
