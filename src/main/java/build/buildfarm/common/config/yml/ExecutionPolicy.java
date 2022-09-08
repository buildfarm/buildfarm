package build.buildfarm.common.config.yml;

public class ExecutionPolicy {
  private String name;
  private ExecutionWrapper executionWrapper;

  /** Required for snakeyaml to parse correctly */
  public ExecutionPolicy() {
    this.name = "";
  }

  public ExecutionPolicy(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public ExecutionWrapper getExecutionWrapper() {
    return executionWrapper;
  }

  public void setExecutionWrapper(ExecutionWrapper executionWrapper) {
    this.executionWrapper = executionWrapper;
  }

  @Override
  public String toString() {
    return "ExecutionPolicy{"
        + "name='"
        + name
        + '\''
        + ", executionWrapper="
        + executionWrapper
        + '}';
  }
}
