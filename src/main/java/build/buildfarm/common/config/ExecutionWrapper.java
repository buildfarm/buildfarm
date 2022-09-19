package build.buildfarm.common.config;

import java.util.List;
import lombok.Data;

@Data
public class ExecutionWrapper {
  private String path;
  private List<String> arguments;
}
