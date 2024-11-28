package persistent.common;

public interface Worker<I, O> extends Destructable {
  O doWork(I request);

  default void destroy() {}
}
