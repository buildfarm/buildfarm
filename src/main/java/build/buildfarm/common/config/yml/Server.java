package build.buildfarm.common.config.yml;

public class Server {
    private String instanceType;
    private String name;
    private boolean actionCacheReadOnly = false;
    private int port = 8980;
    private int prometheusPort = 9090;
    private GrpcMetrics grpcMetrics;
    private int casWriteTimeout = 3600;
    private int bytestreamTimeout = 3600;
    private String sslCertificatePath = null;
    private boolean runDispatchedMonitor;
    private int dispatchedMonitorIntervalSeconds;
    private boolean runOperationQueuer;
    private boolean ensureOutputsPresent;
    private long maxEntrySizeBytes;
    private int maxRequeueAttempts;
    private boolean useDenyList;
    private long grpcTimeout;
    private long maximumActionTimeout;
    private long executeKeepaliveAfterSeconds;
    private boolean recordBesEvents;
    private Admin admin;
    private Metrics metrics;

    private int maxCpu;

    private String clusterId;

    private String cloudRegion;

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

    public boolean isActionCacheReadOnly() {
        return actionCacheReadOnly;
    }

    public void setActionCacheReadOnly(boolean actionCacheReadOnly) {
        this.actionCacheReadOnly = actionCacheReadOnly;
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

    public boolean isRunDispatchedMonitor() {
        return runDispatchedMonitor;
    }

    public void setRunDispatchedMonitor(boolean runDispatchedMonitor) {
        this.runDispatchedMonitor = runDispatchedMonitor;
    }

    public int getDispatchedMonitorIntervalSeconds() {
        return dispatchedMonitorIntervalSeconds;
    }

    public void setDispatchedMonitorIntervalSeconds(int dispatchedMonitorIntervalSeconds) {
        this.dispatchedMonitorIntervalSeconds = dispatchedMonitorIntervalSeconds;
    }

    public boolean isRunOperationQueuer() {
        return runOperationQueuer;
    }

    public void setRunOperationQueuer(boolean runOperationQueuer) {
        this.runOperationQueuer = runOperationQueuer;
    }

    public boolean isEnsureOutputsPresent() {
        return ensureOutputsPresent;
    }

    public void setEnsureOutputsPresent(boolean ensureOutputsPresent) {
        this.ensureOutputsPresent = ensureOutputsPresent;
    }

    public long getMaxEntrySizeBytes() {
        return maxEntrySizeBytes;
    }

    public void setMaxEntrySizeBytes(long maxEntrySizeBytes) {
        this.maxEntrySizeBytes = maxEntrySizeBytes;
    }

    public int getMaxRequeueAttempts() {
        return maxRequeueAttempts;
    }

    public void setMaxRequeueAttempts(int maxRequeueAttempts) {
        this.maxRequeueAttempts = maxRequeueAttempts;
    }

    public boolean isUseDenyList() {
        return useDenyList;
    }

    public void setUseDenyList(boolean useDenyList) {
        this.useDenyList = useDenyList;
    }

    public long getGrpcTimeout() {
        return grpcTimeout;
    }

    public void setGrpcTimeout(long grpcTimeout) {
        this.grpcTimeout = grpcTimeout;
    }

    public long getMaximumActionTimeout() {
        return maximumActionTimeout;
    }

    public void setMaximumActionTimeout(long maximumActionTimeout) {
        this.maximumActionTimeout = maximumActionTimeout;
    }

    public long getExecuteKeepaliveAfterSeconds() {
        return executeKeepaliveAfterSeconds;
    }

    public void setExecuteKeepaliveAfterSeconds(long executeKeepaliveAfterSeconds) {
        this.executeKeepaliveAfterSeconds = executeKeepaliveAfterSeconds;
    }

    public boolean isRecordBesEvents() {
        return recordBesEvents;
    }

    public void setRecordBesEvents(boolean recordBesEvents) {
        this.recordBesEvents = recordBesEvents;
    }

    public Admin getAdmin() {
        return admin;
    }

    public void setAdmin(Admin admin) {
        this.admin = admin;
    }

    public Metrics getMetrics() {
        return metrics;
    }

    public void setMetrics(Metrics metrics) {
        this.metrics = metrics;
    }

    public int getMaxCpu() {
        return maxCpu;
    }

    public void setMaxCpu(int maxCpu) {
        this.maxCpu = maxCpu;
    }

    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public String getCloudRegion() {
        return cloudRegion;
    }

    public void setCloudRegion(String cloudRegion) {
        this.cloudRegion = cloudRegion;
    }

    @Override
    public String toString() {
        return "Server{" +
                "instanceType='" + instanceType + '\'' +
                ", name='" + name + '\'' +
                ", actionCachePolicy='" + actionCacheReadOnly + '\'' +
                ", port=" + port +
                ", prometheusPort=" + prometheusPort +
                ", grpcMetrics=" + grpcMetrics +
                ", casWriteTimeout=" + casWriteTimeout +
                ", bytestreamTimeout=" + bytestreamTimeout +
                ", sslCertificatePath='" + sslCertificatePath + '\'' +
                ", runDispatchedMonitor=" + runDispatchedMonitor +
                ", dispatchedMonitorIntervalSeconds=" + dispatchedMonitorIntervalSeconds +
                ", runOperationQueuer=" + runOperationQueuer +
                ", ensureOutputsPresent=" + ensureOutputsPresent +
                ", maxEntrySizeBytes=" + maxEntrySizeBytes +
                ", maxRequeueAttempts=" + maxRequeueAttempts +
                ", useDenyList=" + useDenyList +
                ", grpcTimeout=" + grpcTimeout +
                ", maximumActionTimeout=" + maximumActionTimeout +
                ", executeKeepaliveAfterSeconds=" + executeKeepaliveAfterSeconds +
                ", recordBesEvents=" + recordBesEvents +
                ", maxCpu=" + maxCpu +
                ", clusterId=" + clusterId +
                ", cloudRegion=" + cloudRegion +
                ", admin=" + admin +
                ", metrics=" + metrics +
                '}';
    }
}
