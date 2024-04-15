package build.buildfarm.instance.server;

import build.buildfarm.common.Scannable;
import com.google.common.base.Predicate;
import com.google.longrunning.Operation;
import lombok.Data;

@Data
public final class OperationsFilter {
  private final Iterable<Scannable<Operation>> locations;
  private final Predicate<Operation> predicate;

  public OperationsFilter(Iterable<Scannable<Operation>> locations) {
    this(locations, o -> true);
  }

  public OperationsFilter(
      Iterable<Scannable<Operation>> locations, Predicate<Operation> predicate) {
    this.locations = locations;
    this.predicate = predicate;
  }
}
