package build.buildfarm.common.config.yml;

public class Server {
    private String instanceType;
    private String name;
    private String digestFunction;
    private String actionCachePolicy;
    private int port;
    private int prometheusPort;
    private GrpcMetrics grpcMetrics;
    private int casWriteTimeout;
    private int bytestreamTimeout;
    private String sslCertificatePath;

    public String getInstanceType() {
        return instanceType;
    }

    public void setInstanceType(String instanceType) {
        this.instanceType = instanceType;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDigestFunction() {
        return digestFunction;
    }

    public void setDigestFunction(String digestFunction) {
        this.digestFunction = digestFunction;
    }

    public String getActionCachePolicy() {
        return actionCachePolicy;
    }

    public void setActionCachePolicy(String actionCachePolicy) {
        this.actionCachePolicy = actionCachePolicy;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getPrometheusPort() {
        return prometheusPort;
    }

    public void setPrometheusPort(int prometheusPort) {
        this.prometheusPort = prometheusPort;
    }

    public GrpcMetrics getGrpcMetrics() {
        return grpcMetrics;
    }

    public void setGrpcMetrics(GrpcMetrics grpcMetrics) {
        this.grpcMetrics = grpcMetrics;
    }

    public int getCasWriteTimeout() {
        return casWriteTimeout;
    }

    public void setCasWriteTimeout(int casWriteTimeout) {
        this.casWriteTimeout = casWriteTimeout;
    }

    public int getBytestreamTimeout() {
        return bytestreamTimeout;
    }

    public void setBytestreamTimeout(int bytestreamTimeout) {
        this.bytestreamTimeout = bytestreamTimeout;
    }

    public String getSslCertificatePath() {
        return sslCertificatePath;
    }

    public void setSslCertificatePath(String sslCertificatePath) {
        this.sslCertificatePath = sslCertificatePath;
    }

    @Override
    public String toString() {
        return "Server{" +
                "instanceType='" + instanceType + '\'' +
                ", name='" + name + '\'' +
                ", digestFunction='" + digestFunction + '\'' +
                ", actionCachePolicy='" + actionCachePolicy + '\'' +
                ", port=" + port +
                ", prometheusPort=" + prometheusPort +
                ", grpcMetrics=" + grpcMetrics +
                ", casWriteTimeout=" + casWriteTimeout +
                ", bytestreamTimeout=" + bytestreamTimeout +
                ", sslCertificatePath='" + sslCertificatePath + '\'' +
                '}';
    }
}
