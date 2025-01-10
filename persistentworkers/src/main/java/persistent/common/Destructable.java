package persistent.common;

// Closeable.close() has a checked exception (IOException)
public interface Destructable {
  void destroy();
}
