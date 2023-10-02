package build.buildfarm.common.config;

import lombok.Data;

@Data
public class Admin {
  public enum DEPLOYMENT_ENVIRONMENT {
    AWS,
    GCP
  }

  private DEPLOYMENT_ENVIRONMENT deploymentEnvironment;
  private String clusterEndpoint;
  // This configuration is deprecated but is left here for backwards compatibility. Use
  // worker:gracefulShutdownSeconds instead.
  private boolean enableGracefulShutdown;
}
