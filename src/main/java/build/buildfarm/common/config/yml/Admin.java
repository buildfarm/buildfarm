package build.buildfarm.common.config.yml;

import lombok.Data;

@Data
public class Admin {
  private String deploymentEnvironment;
  private String clusterEndpoint;
  private boolean enableGracefulShutdown;
}
