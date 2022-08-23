package build.buildfarm.common.config.yml;

import java.util.Arrays;

public class Backplane {
    private String type;
    private String redisURI;
    private long jedisPoolMaxTotal;
    private String workersHashName;
    private String workerChannel;
    private String actionCachePrefix;
    private long actionCacheExpire;
    private String actionBlacklistPrefix;
    private long actionBlacklistExpire;
    private String invocationBlacklistPrefix;
    private String operationPrefix;
    private long operationExpire;
    private String preQueuedOperationsListName;
    private String processingListName;
    private String processingPrefix;
    private long processingTimeoutMillis;
    private String queuedOperationsListName;
    private String dispatchingPrefix;
    private long dispatchingTimeoutMillis;
    private String dispatchedOperationsHashName;
    private String operationChannelPrefix;
    private String casPrefix;
    private long casExpire;
    private boolean subscribeToBackplane;
    private boolean runFailsafeOperation;
    private long maxQueueDepth;
    private long maxPreQueueDepth;
    private String redisQueueType;
    private Queue[] queues;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getRedisURI() {
        return redisURI;
    }

    public void setRedisURI(String redisURI) {
        this.redisURI = redisURI;
    }

    public long getJedisPoolMaxTotal() {
        return jedisPoolMaxTotal;
    }

    public void setJedisPoolMaxTotal(long jedisPoolMaxTotal) {
        this.jedisPoolMaxTotal = jedisPoolMaxTotal;
    }

    public String getWorkersHashName() {
        return workersHashName;
    }

    public void setWorkersHashName(String workersHashName) {
        this.workersHashName = workersHashName;
    }

    public String getWorkerChannel() {
        return workerChannel;
    }

    public void setWorkerChannel(String workerChannel) {
        this.workerChannel = workerChannel;
    }

    public String getActionCachePrefix() {
        return actionCachePrefix;
    }

    public void setActionCachePrefix(String actionCachePrefix) {
        this.actionCachePrefix = actionCachePrefix;
    }

    public long getActionCacheExpire() {
        return actionCacheExpire;
    }

    public void setActionCacheExpire(long actionCacheExpire) {
        this.actionCacheExpire = actionCacheExpire;
    }

    public String getActionBlacklistPrefix() {
        return actionBlacklistPrefix;
    }

    public void setActionBlacklistPrefix(String actionBlacklistPrefix) {
        this.actionBlacklistPrefix = actionBlacklistPrefix;
    }

    public long getActionBlacklistExpire() {
        return actionBlacklistExpire;
    }

    public void setActionBlacklistExpire(long actionBlacklistExpire) {
        this.actionBlacklistExpire = actionBlacklistExpire;
    }

    public String getInvocationBlacklistPrefix() {
        return invocationBlacklistPrefix;
    }

    public void setInvocationBlacklistPrefix(String invocationBlacklistPrefix) {
        this.invocationBlacklistPrefix = invocationBlacklistPrefix;
    }

    public String getOperationPrefix() {
        return operationPrefix;
    }

    public void setOperationPrefix(String operationPrefix) {
        this.operationPrefix = operationPrefix;
    }

    public long getOperationExpire() {
        return operationExpire;
    }

    public void setOperationExpire(long operationExpire) {
        this.operationExpire = operationExpire;
    }

    public String getPreQueuedOperationsListName() {
        return preQueuedOperationsListName;
    }

    public void setPreQueuedOperationsListName(String preQueuedOperationsListName) {
        this.preQueuedOperationsListName = preQueuedOperationsListName;
    }

    public String getProcessingListName() {
        return processingListName;
    }

    public void setProcessingListName(String processingListName) {
        this.processingListName = processingListName;
    }

    public String getProcessingPrefix() {
        return processingPrefix;
    }

    public void setProcessingPrefix(String processingPrefix) {
        this.processingPrefix = processingPrefix;
    }

    public long getProcessingTimeoutMillis() {
        return processingTimeoutMillis;
    }

    public void setProcessingTimeoutMillis(long processingTimeoutMillis) {
        this.processingTimeoutMillis = processingTimeoutMillis;
    }

    public String getQueuedOperationsListName() {
        return queuedOperationsListName;
    }

    public void setQueuedOperationsListName(String queuedOperationsListName) {
        this.queuedOperationsListName = queuedOperationsListName;
    }

    public String getDispatchingPrefix() {
        return dispatchingPrefix;
    }

    public void setDispatchingPrefix(String dispatchingPrefix) {
        this.dispatchingPrefix = dispatchingPrefix;
    }

    public long getDispatchingTimeoutMillis() {
        return dispatchingTimeoutMillis;
    }

    public void setDispatchingTimeoutMillis(long dispatchingTimeoutMillis) {
        this.dispatchingTimeoutMillis = dispatchingTimeoutMillis;
    }

    public String getDispatchedOperationsHashName() {
        return dispatchedOperationsHashName;
    }

    public void setDispatchedOperationsHashName(String dispatchedOperationsHashName) {
        this.dispatchedOperationsHashName = dispatchedOperationsHashName;
    }

    public String getOperationChannelPrefix() {
        return operationChannelPrefix;
    }

    public void setOperationChannelPrefix(String operationChannelPrefix) {
        this.operationChannelPrefix = operationChannelPrefix;
    }

    public String getCasPrefix() {
        return casPrefix;
    }

    public void setCasPrefix(String casPrefix) {
        this.casPrefix = casPrefix;
    }

    public long getCasExpire() {
        return casExpire;
    }

    public void setCasExpire(long casExpire) {
        this.casExpire = casExpire;
    }

    public boolean isSubscribeToBackplane() {
        return subscribeToBackplane;
    }

    public void setSubscribeToBackplane(boolean subscribeToBackplane) {
        this.subscribeToBackplane = subscribeToBackplane;
    }

    public boolean isRunFailsafeOperation() {
        return runFailsafeOperation;
    }

    public void setRunFailsafeOperation(boolean runFailsafeOperation) {
        this.runFailsafeOperation = runFailsafeOperation;
    }

    public long getMaxQueueDepth() {
        return maxQueueDepth;
    }

    public void setMaxQueueDepth(long maxQueueDepth) {
        this.maxQueueDepth = maxQueueDepth;
    }

    public long getMaxPreQueueDepth() {
        return maxPreQueueDepth;
    }

    public void setMaxPreQueueDepth(long maxPreQueueDepth) {
        this.maxPreQueueDepth = maxPreQueueDepth;
    }

    public String getRedisQueueType() {
        return redisQueueType;
    }

    public void setRedisQueueType(String redisQueueType) {
        this.redisQueueType = redisQueueType;
    }

    public Queue[] getQueues() {
        return queues;
    }

    public void setQueues(Queue[] queues) {
        this.queues = queues;
    }

    @Override
    public String toString() {
        return "Backplane{" +
                "type='" + type + '\'' +
                ", redisURI='" + redisURI + '\'' +
                ", jedisPoolMaxTotal=" + jedisPoolMaxTotal +
                ", workersHashName='" + workersHashName + '\'' +
                ", workerChannel='" + workerChannel + '\'' +
                ", actionCachePrefix='" + actionCachePrefix + '\'' +
                ", actionCacheExpire=" + actionCacheExpire +
                ", actionBlacklistPrefix='" + actionBlacklistPrefix + '\'' +
                ", actionBlacklistExpire=" + actionBlacklistExpire +
                ", invocationBlacklistPrefix='" + invocationBlacklistPrefix + '\'' +
                ", operationPrefix='" + operationPrefix + '\'' +
                ", operationExpire=" + operationExpire +
                ", preQueuedOperationsListName='" + preQueuedOperationsListName + '\'' +
                ", processingListName='" + processingListName + '\'' +
                ", processingPrefix='" + processingPrefix + '\'' +
                ", processingTimeoutMillis=" + processingTimeoutMillis +
                ", queuedOperationsListName='" + queuedOperationsListName + '\'' +
                ", dispatchingPrefix='" + dispatchingPrefix + '\'' +
                ", dispatchingTimeoutMillis=" + dispatchingTimeoutMillis +
                ", dispatchedOperationsHashName='" + dispatchedOperationsHashName + '\'' +
                ", operationChannelPrefix='" + operationChannelPrefix + '\'' +
                ", casPrefix='" + casPrefix + '\'' +
                ", casExpire=" + casExpire +
                ", subscribeToBackplane=" + subscribeToBackplane +
                ", runFailsafeOperation=" + runFailsafeOperation +
                ", maxQueueDepth=" + maxQueueDepth +
                ", maxPreQueueDepth=" + maxPreQueueDepth +
                ", redisQueueType='" + redisQueueType + '\'' +
                ", queues=" + Arrays.toString(queues) +
                '}';
    }
}
