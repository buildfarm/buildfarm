package build.buildfarm.instance.shard;

import build.buildfarm.instance.Watcher;
import java.time.Instant;

abstract class TimedWatcher<T> implements Watcher<T> {
  private Instant expiresAt;

  TimedWatcher(Instant expiresAt) {
    reset(expiresAt);
  }

  void reset(Instant expiresAt) {
    this.expiresAt = expiresAt;
  }

  boolean isExpiredAt(Instant now) {
    return now.isAfter(expiresAt);
  }

  Instant getExpiresAt() {
    return expiresAt;
  }
}
