package build.buildfarm.common.config.yml;

public class Capabilities {
  private boolean cas = true;
  private boolean execution = true;

  public boolean isCas() {
    return cas;
  }

  public void setCas(boolean cas) {
    this.cas = cas;
  }

  public boolean isExecution() {
    return execution;
  }

  public void setExecution(boolean execution) {
    this.execution = execution;
  }

  @Override
  public String toString() {
    return "Capabilities{" + "cas=" + cas + ", execution=" + execution + '}';
  }
}
