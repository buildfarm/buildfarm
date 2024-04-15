package build.buildfarm.common.function;

import java.util.function.Consumer;
import lombok.Getter;

public class CountingConsumer<T> implements Consumer<T> {
  private final Consumer<T> delegate;
  @Getter private int count = 0;

  public CountingConsumer(Consumer<T> delegate) {
    this.delegate = delegate;
  }

  @Override
  public void accept(T t) {
    count++;
    delegate.accept(t);
  }
}
