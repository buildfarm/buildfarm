package build.buildfarm.common.config;

import lombok.Data;

@Data
public class DequeueMatchSettings {
  private boolean acceptEverything;
  private boolean allowUnmatched;
}
