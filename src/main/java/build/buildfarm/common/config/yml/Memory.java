package build.buildfarm.common.config.yml;

import build.bazel.remote.execution.v2.Platform;

public class Memory {
    private int listOperationsDefaultPageSize;
    private int listOperationsMaxPageSize;
    private int treeDefaultPageSize;
    private int treeMaxPageSize;
    private int operationPollTimeout;
    private int operationCompletedDelay;
    private boolean delegateCas;
    private String target;
    private int deadlineAfterSeconds;
    private boolean streamStdout;
    private boolean streamStderr;
    private String casPolicy;
    private int treePageSize;
    private Platform platform;
    private Platform defaultPlatform;

    public int getListOperationsDefaultPageSize() {
        return listOperationsDefaultPageSize;
    }

    public void setListOperationsDefaultPageSize(int listOperationsDefaultPageSize) {
        this.listOperationsDefaultPageSize = listOperationsDefaultPageSize;
    }

    public int getListOperationsMaxPageSize() {
        return listOperationsMaxPageSize;
    }

    public void setListOperationsMaxPageSize(int listOperationsMaxPageSize) {
        this.listOperationsMaxPageSize = listOperationsMaxPageSize;
    }

    public int getTreeDefaultPageSize() {
        return treeDefaultPageSize;
    }

    public void setTreeDefaultPageSize(int treeDefaultPageSize) {
        this.treeDefaultPageSize = treeDefaultPageSize;
    }

    public int getTreeMaxPageSize() {
        return treeMaxPageSize;
    }

    public void setTreeMaxPageSize(int treeMaxPageSize) {
        this.treeMaxPageSize = treeMaxPageSize;
    }

    public int getOperationPollTimeout() {
        return operationPollTimeout;
    }

    public void setOperationPollTimeout(int operationPollTimeout) {
        this.operationPollTimeout = operationPollTimeout;
    }

    public int getOperationCompletedDelay() {
        return operationCompletedDelay;
    }

    public void setOperationCompletedDelay(int operationCompletedDelay) {
        this.operationCompletedDelay = operationCompletedDelay;
    }

    public boolean isDelegateCas() {
        return delegateCas;
    }

    public void setDelegateCas(boolean delegateCas) {
        this.delegateCas = delegateCas;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public int getDeadlineAfterSeconds() {
        return deadlineAfterSeconds;
    }

    public void setDeadlineAfterSeconds(int deadlineAfterSeconds) {
        this.deadlineAfterSeconds = deadlineAfterSeconds;
    }

    public boolean isStreamStdout() {
        return streamStdout;
    }

    public void setStreamStdout(boolean streamStdout) {
        this.streamStdout = streamStdout;
    }

    public boolean isStreamStderr() {
        return streamStderr;
    }

    public void setStreamStderr(boolean streamStderr) {
        this.streamStderr = streamStderr;
    }

    public String getCasPolicy() {
        return casPolicy;
    }

    public void setCasPolicy(String casPolicy) {
        this.casPolicy = casPolicy;
    }

    public int getTreePageSize() {
        return treePageSize;
    }

    public void setTreePageSize(int treePageSize) {
        this.treePageSize = treePageSize;
    }

    public Platform getPlatform() {
        return platform;
    }

    public void setPlatform(Platform platform) {
        this.platform = platform;
    }

    public Platform getDefaultPlatform() {
        return defaultPlatform;
    }

    public void setDefaultPlatform(Platform defaultPlatform) {
        this.defaultPlatform = defaultPlatform;
    }

    @Override
    public String toString() {
        return "Memory{" +
                "listOperationsDefaultPageSize=" + listOperationsDefaultPageSize +
                ", listOperationsMaxPageSize=" + listOperationsMaxPageSize +
                ", treeDefaultPageSize=" + treeDefaultPageSize +
                ", treeMaxPageSize=" + treeMaxPageSize +
                ", operationPollTimeout=" + operationPollTimeout +
                ", operationCompletedDelay=" + operationCompletedDelay +
                ", delegateCas=" + delegateCas +
                ", target='" + target + '\'' +
                ", deadlineAfterSeconds=" + deadlineAfterSeconds +
                ", streamStdout=" + streamStdout +
                ", streamStderr=" + streamStderr +
                ", casPolicy='" + casPolicy + '\'' +
                ", treePageSize=" + treePageSize +
                ", platform=" + platform +
                ", defaultPlatform=" + defaultPlatform +
                '}';
    }
}
