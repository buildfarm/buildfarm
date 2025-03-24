package build.buildfarm.common.config;

import lombok.Data;

@Data
public class Capabilities {
  private boolean cas;
  private boolean execution;
}
