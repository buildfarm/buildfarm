package persistent.common;

import java.util.HashMap;
import java.util.function.Function;

public class MapPool<K, V> implements ObjectPool<K, V> {
  private final HashMap<K, V> map;

  private final Function<K, V> objFactory;

  public MapPool(Function<K, V> factory) {
    this.map = new HashMap<>();
    this.objFactory = factory;
  }

  @Override
  public V obtain(K key) {
    if (map.containsKey(key)) {
      return map.remove(key);
    }
    return objFactory.apply(key);
  }

  @Override
  public void release(K key, V obj) {
    map.put(key, obj);
  }
}
