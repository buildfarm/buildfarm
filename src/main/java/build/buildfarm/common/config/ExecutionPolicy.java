package build.buildfarm.common.config;

import lombok.Data;

@Data
public class ExecutionPolicy {
  private String name;
  private boolean prioritized;
  private ExecutionWrapper executionWrapper;

  /** Required for snakeyaml to parse correctly */
  public ExecutionPolicy() {
    this("");
  }

  public ExecutionPolicy(String name) {
    this.name = name;
  }
}
