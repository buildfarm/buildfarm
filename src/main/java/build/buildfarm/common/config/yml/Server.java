package build.buildfarm.common.config.yml;

public class Server {
    private static String name;
    private static String digestFunction;
    private static String actionCachePolicy;
    private static int port;
    private static int prometheusPort;
    private static GrpcMetrics grpcMetrics;
    private static int casWriteTimeout;
    private static int bytestreamTimeout;

    public String getName() { return name; }
    public void setName(String value) { this.name = value; }
    public String getDigestFunction() { return digestFunction; }
    public void setDigestFunction(String value) { this.digestFunction = value; }
    public String getActionCachePolicy() { return actionCachePolicy; }
    public void setActionCachePolicy(String value) { this.actionCachePolicy = value; }
    public int getPort() { return port; }
    public void setPort(int value) { this.port = value; }
    public int getPrometheusPort() { return prometheusPort; }
    public void setPrometheusPort(int value) { this.prometheusPort = value; }
    public GrpcMetrics getGrpcMetrics() { return grpcMetrics; }
    public void setGrpcMetrics(GrpcMetrics value) { this.grpcMetrics = value; }
    public int getCasWriteTimeout() { return casWriteTimeout; }
    public void setCasWriteTimeout(int value) { this.casWriteTimeout = value; }
    public int getBytestreamTimeout() { return bytestreamTimeout; }
    public void setBytestreamTimeout(int value) { this.bytestreamTimeout = value; }

    public String toString() {
        return "server:\n" +
                "\tname: " + name + "\n" +
                "\tdigestFunction: " + digestFunction + "\n" +
                "\tactionCachePolicy: " + actionCachePolicy + "\n" +
                "\tport: " + port + "\n" +
                "\tprometheusPort: " + prometheusPort + "\n" +
                "\tgprcMetrics: " + grpcMetrics + "\n" +
                "\tcasWriteTimeout: " + casWriteTimeout + "\n" +
                "\tbytestreamTimeout: " + bytestreamTimeout + "\n";
    }
}
