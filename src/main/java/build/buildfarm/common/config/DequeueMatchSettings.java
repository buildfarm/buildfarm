package build.buildfarm.common.config;

import build.bazel.remote.execution.v2.Platform;
import lombok.Data;

@Data
public class DequeueMatchSettings {
  private boolean acceptEverything = true;
  private boolean allowUnmatched = false;
  private Platform platform = Platform.getDefaultInstance();
}
