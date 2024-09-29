package build.buildfarm.common.config;

import lombok.Data;

@Data
public class ExecutionWrapper {
  private String path;
  private String[] arguments = new String[0];
}
