package build.buildfarm.common.config;

import lombok.Data;

@Data
public class Capabilities {
  private boolean cas = true;
  private boolean execution = true;
}
