package build.buildfarm.common.config.yml;

public class Admin {
    private String deploymentEnvironment;
    private String clusterEndpoint;

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

    @Override
    public String toString() {
        return "Admin{" +
                "deploymentEnvironment='" + deploymentEnvironment + '\'' +
                ", clusterEndpoint='" + clusterEndpoint + '\'' +
                '}';
    }
}
