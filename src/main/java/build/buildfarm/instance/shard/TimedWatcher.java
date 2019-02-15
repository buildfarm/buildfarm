package build.buildfarm.instance.shard;

import build.buildfarm.common.Watcher;
import java.time.Instant;

abstract class TimedWatcher implements Watcher {
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
