package build.buildfarm.common.config.yml;

import lombok.Data;

@Data
public class ExecutionPolicy {
  private String name;
  private ExecutionWrapper executionWrapper;

  /** Required for snakeyaml to parse correctly */
  public ExecutionPolicy() {
    this("");
  }

  public ExecutionPolicy(String name) {
    this.name = name;
  }
}
