package build.buildfarm.common.config.yml;

public class Admin {
    private String deploymentEnvironment;
    private String clusterId;
    private String clusterEndpoint;
    private String region;

    public String getDeploymentEnvironment() {
        return deploymentEnvironment;
    }

    public void setDeploymentEnvironment(String deploymentEnvironment) {
        this.deploymentEnvironment = deploymentEnvironment;
    }

    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public String getClusterEndpoint() {
        return clusterEndpoint;
    }

    public void setClusterEndpoint(String clusterEndpoint) {
        this.clusterEndpoint = clusterEndpoint;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    @Override
    public String toString() {
        return "Admin{" +
                "deploymentEnvironment='" + deploymentEnvironment + '\'' +
                ", clusterId='" + clusterId + '\'' +
                ", clusterEndpoint='" + clusterEndpoint + '\'' +
                ", region='" + region + '\'' +
                '}';
    }
}
