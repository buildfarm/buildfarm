package build.buildfarm.common.config.yml;

import lombok.Data;

@Data
public class Admin {
  public enum DEPLOYMENT_ENVIRONMENT {
    AWS,
    GCP
  }

  private DEPLOYMENT_ENVIRONMENT deploymentEnvironment;
  private String clusterEndpoint;
  private boolean enableGracefulShutdown;
}
