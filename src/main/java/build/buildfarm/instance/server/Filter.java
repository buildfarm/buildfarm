package build.buildfarm.instance.server;

import build.buildfarm.common.Scannable;
import com.google.common.base.Predicate;
import lombok.Data;

@Data
public final class Filter<T> {
  private final Iterable<Scannable<T>> bounds;
  private final Predicate<T> predicate;

  public Filter(Iterable<Scannable<T>> locations) {
    this(locations, o -> true);
  }

  public Filter(Iterable<Scannable<T>> bounds, Predicate<T> predicate) {
    this.bounds = bounds;
    this.predicate = predicate;
  }
}
