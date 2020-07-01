package build.buildfarm.common;

import java.util.logging.LogManager;
import javax.annotation.concurrent.GuardedBy;

public class WaitingLogManager extends LogManager {
  static WaitingLogManager instance;

  @GuardedBy("this")
  private boolean resetCalled = false;

  @GuardedBy("this")
  private boolean released = false;

  public WaitingLogManager() {
    instance = this;
  }

  @Override
  public synchronized void reset() {
    resetCalled = true;
    reset0();
  }

  @GuardedBy("this")
  private void reset0() {
    if (released && resetCalled) {
      super.reset();
    }
  }

  @SuppressWarnings("GuardedBy")
  private synchronized void releaseReset() {
    released = true;
    instance.reset0();
  }

  public static void release() {
    instance.releaseReset();
  }
}
