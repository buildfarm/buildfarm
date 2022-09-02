package build.buildfarm.common.config.yml;

public class DequeueMatchSettings {
  private boolean acceptEverything = true;

  private boolean allowUnmatched = false;

  public boolean isAcceptEverything() {
    return acceptEverything;
  }

  public void setAcceptEverything(boolean acceptEverything) {
    this.acceptEverything = acceptEverything;
  }

  public boolean isAllowUnmatched() {
    return allowUnmatched;
  }

  public void setAllowUnmatched(boolean allowUnmatched) {
    this.allowUnmatched = allowUnmatched;
  }

  @Override
  public String toString() {
    return "DequeueMatchSettings{"
        + "acceptEverything="
        + acceptEverything
        + ", allowUnmatched="
        + allowUnmatched
        + '}';
  }
}
