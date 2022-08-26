package build.buildfarm.common.config.yml;

public class Admin {
    private String deploymentEnvironment;
    private String clusterEndpoint;

    private boolean enableGracefulShutdown;

    public String getDeploymentEnvironment() {
        return deploymentEnvironment;
    }

    public void setDeploymentEnvironment(String deploymentEnvironment) {
        this.deploymentEnvironment = deploymentEnvironment;
    }

    public String getClusterEndpoint() {
        return clusterEndpoint;
    }

    public void setClusterEndpoint(String clusterEndpoint) {
        this.clusterEndpoint = clusterEndpoint;
    }

    public boolean isEnableGracefulShutdown() {
        return enableGracefulShutdown;
    }

    public void setEnableGracefulShutdown(boolean enableGracefulShutdown) {
        this.enableGracefulShutdown = enableGracefulShutdown;
    }

    @Override
    public String toString() {
        return "Admin{" +
                "deploymentEnvironment='" + deploymentEnvironment + '\'' +
                ", clusterEndpoint='" + clusterEndpoint + '\'' +
                ", enableGracefulShutdown=" + enableGracefulShutdown +
                '}';
    }
}
