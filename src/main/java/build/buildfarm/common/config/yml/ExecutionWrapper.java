package build.buildfarm.common.config.yml;

import java.util.Arrays;

public class ExecutionWrapper {
  private String path;
  private String[] arguments = new String[0];

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public String[] getArguments() {
    return arguments;
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
