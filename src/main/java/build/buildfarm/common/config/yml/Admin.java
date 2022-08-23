package build.buildfarm.common.config.yml;

public class Admin {
    private String deploymentEnvironment;
    private String clusterID;
    private String clusterEndpoint;
    private String region;

    public String getDeploymentEnvironment() {
        return deploymentEnvironment;
    }

    public void setDeploymentEnvironment(String deploymentEnvironment) {
        this.deploymentEnvironment = deploymentEnvironment;
    }

    public String getClusterID() {
        return clusterID;
    }

    public void setClusterID(String clusterID) {
        this.clusterID = clusterID;
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
                ", clusterID='" + clusterID + '\'' +
                ", clusterEndpoint='" + clusterEndpoint + '\'' +
                ", region='" + region + '\'' +
                '}';
    }
}
