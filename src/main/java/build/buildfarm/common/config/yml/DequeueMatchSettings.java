package build.buildfarm.common.config.yml;

import lombok.Data;

@Data
public class DequeueMatchSettings {
  private boolean acceptEverything = true;
  private boolean allowUnmatched = false;
}
