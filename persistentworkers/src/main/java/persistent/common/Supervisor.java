package persistent.common;

public interface Supervisor<K, V extends Destructable, P> {
  V create(K k) throws Exception;

  P wrap(V v);

  boolean validateObject(K key, P p);

  void destroyObject(K key, P p);
}
