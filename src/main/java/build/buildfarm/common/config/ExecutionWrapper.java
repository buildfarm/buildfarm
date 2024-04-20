package build.buildfarm.common.config;

import java.util.Arrays;
import lombok.Getter;

@Getter
public class ExecutionWrapper {
  private String path;
  private String[] arguments = new String[0];

  public void setPath(String path) {
    this.path = path;
  }

  public void setArguments(String[] arguments) {
    this.arguments = arguments;
  }

  @Override
  public String toString() {
    return "ExecutionWrapper{"
        + "path='"
        + path
        + '\''
        + ", arguments="
        + Arrays.toString(arguments)
        + '}';
  }
}
