package build.buildfarm.common.config.yml;

import java.util.Arrays;
import java.util.List;

public class Worker {
    private long port = 8981;
    private String publicName;
    private Capabilities capabilities = new Capabilities();
    private String root = "/tmp/worker";
    private long inlineContentLimit = 1048567; //1024 * 1024
    private long operationPollPeriod = 1;
    private DequeueMatchSettings dequeueMatchSettings = new DequeueMatchSettings();
    private Cas cas = new Cas();
    private long executeStageWidth = 1;
    private long inputFetchStageWidth = 1;
    private long inputFetchDeadline = 60;
    private boolean linkInputDirectories = true;
    private List<String> realInputDirectories = Arrays.asList("external");
    private String execOwner;

    private int hexBucketLevels = 0;

    public long getPort() {
        return port;
    }

    public void setPort(long port) {
        this.port = port;
    }

    public String getPublicName() {
        return publicName;
    }

    public void setPublicName(String publicName) {
        this.publicName = publicName;
    }

    public Capabilities getCapabilities() {
        return capabilities;
    }

    public void setCapabilities(Capabilities capabilities) {
        this.capabilities = capabilities;
    }

    public String getRoot() {
        return root;
    }

    public void setRoot(String root) {
        this.root = root;
    }

    public long getInlineContentLimit() {
        return inlineContentLimit;
    }

    public void setInlineContentLimit(long inlineContentLimit) {
        this.inlineContentLimit = inlineContentLimit;
    }

    public long getOperationPollPeriod() {
        return operationPollPeriod;
    }

    public void setOperationPollPeriod(long operationPollPeriod) {
        this.operationPollPeriod = operationPollPeriod;
    }

    public DequeueMatchSettings getDequeueMatchSettings() {
        return dequeueMatchSettings;
    }

    public void setDequeueMatchSettings(DequeueMatchSettings dequeueMatchSettings) {
        this.dequeueMatchSettings = dequeueMatchSettings;
    }

    public Cas getCas() {
        return cas;
    }

    public void setCas(Cas cas) {
        this.cas = cas;
    }

    public long getExecuteStageWidth() {
        return executeStageWidth;
    }

    public void setExecuteStageWidth(long executeStageWidth) {
        this.executeStageWidth = executeStageWidth;
    }

    public long getInputFetchStageWidth() {
        return inputFetchStageWidth;
    }

    public void setInputFetchStageWidth(long inputFetchStageWidth) {
        this.inputFetchStageWidth = inputFetchStageWidth;
    }

    public long getInputFetchDeadline() {
        return inputFetchDeadline;
    }

    public void setInputFetchDeadline(long inputFetchDeadline) {
        this.inputFetchDeadline = inputFetchDeadline;
    }

    public boolean isLinkInputDirectories() {
        return linkInputDirectories;
    }

    public void setLinkInputDirectories(boolean linkInputDirectories) {
        this.linkInputDirectories = linkInputDirectories;
    }

    public List<String> getRealInputDirectories() {
        return realInputDirectories;
    }

    public void setRealInputDirectories(List<String> realInputDirectories) {
        this.realInputDirectories = realInputDirectories;
    }

    public String getExecOwner() {
        return execOwner;
    }

    public void setExecOwner(String execOwner) {
        this.execOwner = execOwner;
    }

    public int getHexBucketLevels() {
        return hexBucketLevels;
    }

    public void setHexBucketLevels(int hexBucketLevels) {
        this.hexBucketLevels = hexBucketLevels;
    }

    @Override
    public String toString() {
        return "Worker{" +
                "port=" + port +
                ", publicName='" + publicName + '\'' +
                ", capabilities=" + capabilities +
                ", root='" + root + '\'' +
                ", inlineContentLimit=" + inlineContentLimit +
                ", operationPollPeriod=" + operationPollPeriod +
                ", dequeueMatchSettings=" + dequeueMatchSettings +
                ", cas=" + cas +
                ", executeStageWidth=" + executeStageWidth +
                ", inputFetchStageWidth=" + inputFetchStageWidth +
                ", inputFetchDeadline=" + inputFetchDeadline +
                ", linkInputDirectories=" + linkInputDirectories +
                ", realInputDirectories='" + realInputDirectories + '\'' +
                ", execOwner='" + execOwner + '\'' +
                ", hexBucketLevels=" + hexBucketLevels +
                '}';
    }
}
