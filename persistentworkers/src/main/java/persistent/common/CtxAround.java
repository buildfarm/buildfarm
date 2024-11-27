package persistent.common;

public interface CtxAround<T> {
  T get();

  // i.e. no context
  class Id<T> implements CtxAround<T> {
    T value;

    public Id(T value) {
      this.value = value;
    }

    @Override
    public T get() {
      return value;
    }

    public static <T> Id<T> of(T value) {
      return new Id<>(value);
    }

    @Override
    public boolean equals(Object obj) {
      return obj.equals(this.value);
    }
  }
}
