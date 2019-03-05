package build.buildfarm.instance.shard;

import com.google.longrunning.Operation;
import java.time.Instant;

class UnobservableWatcher extends TimedWatcher {
  UnobservableWatcher() {
    this(/* expiresAt=*/ Instant.now());
  }

  UnobservableWatcher(Instant expiresAt) {
    super(expiresAt);
  }

  @Override
  public void observe(Operation operation) {
    throw new UnsupportedOperationException();
  }
}

