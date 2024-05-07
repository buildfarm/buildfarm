package build.buildfarm.common.config;

import lombok.Data;

@Data
public class ExecutionWrapper {
  String path;
  String[] arguments = new String[0];
}
