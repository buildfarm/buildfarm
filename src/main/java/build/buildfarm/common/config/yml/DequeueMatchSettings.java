package build.buildfarm.common.config.yml;

public class DequeueMatchSettings {
  private String platform;
  private boolean acceptEverything = true;

  private boolean allowUnmatched = false;

  public String getPlatform() {
    return platform;
  }

  public void setPlatform(String platform) {
    this.platform = platform;
  }

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
        + "platform='"
        + platform
        + '\''
        + ", acceptEverything="
        + acceptEverything
        + ", allowUnmatched="
        + allowUnmatched
        + '}';
  }
}
