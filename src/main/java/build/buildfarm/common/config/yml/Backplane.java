package build.buildfarm.common.config.yml;

import java.util.Arrays;

public class Backplane {
    private String type = "SHARD";
    private String redisUri;
    private int jedisPoolMaxTotal = 4000;
    private String workersHashName = "Workers";
    private String workerChannel = "WorkerChannel";
    private String actionCachePrefix = "ActionCache";
    private int actionCacheExpire = 2419200;  //4 Weeks
    private String actionBlacklistPrefix = "ActionBlacklist";
    private int actionBlacklistExpire = 3600;  //1 Hour;
    private String invocationBlacklistPrefix = "InvocationBlacklist";
    private String operationPrefix = "Operation";
    private int operationExpire = 604800;  //1 Week
    private String preQueuedOperationsListName = "{Arrival}:PreQueuedOperations";
    private String processingListName = "{Arrival}:ProcessingOperations";
    private String processingPrefix = "Processing";
    private int processingTimeoutMillis = 20000;
    private String queuedOperationsListName = "{Execution}:QueuedOperations";
    private String dispatchingPrefix = "Dispatching";
    private int dispatchingTimeoutMillis = 10000;
    private String dispatchedOperationsHashName = "DispatchedOperations";
    private String operationChannelPrefix = "OperationChannel";
    private String casPrefix = "ContentAddressableStorage";
    private int casExpire = 604800; //1 Week
    private boolean subscribeToBackplane = true;
    private boolean runFailsafeOperation = true;
    private int maxQueueDepth = 100000;
    private int maxPreQueueDepth = 1000000;
    private String redisQueueType = "NORMAL";
    private Queue[] queues;
    private String redisPassword;
    private int timeout = 10000;
    private String[] redisNodes;
    private int maxAttempts = 20;
    private boolean cacheCas = false;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getRedisUri() {
        return redisUri;
    }

    public void setRedisUri(String redisUri) {
        this.redisUri = redisUri;
    }

    public int getJedisPoolMaxTotal() {
        return jedisPoolMaxTotal;
    }

    public void setJedisPoolMaxTotal(int jedisPoolMaxTotal) {
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

    public int getActionCacheExpire() {
        return actionCacheExpire;
    }

    public void setActionCacheExpire(int actionCacheExpire) {
        this.actionCacheExpire = actionCacheExpire;
    }

    public String getActionBlacklistPrefix() {
        return actionBlacklistPrefix;
    }

    public void setActionBlacklistPrefix(String actionBlacklistPrefix) {
        this.actionBlacklistPrefix = actionBlacklistPrefix;
    }

    public int getActionBlacklistExpire() {
        return actionBlacklistExpire;
    }

    public void setActionBlacklistExpire(int actionBlacklistExpire) {
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

    public int getOperationExpire() {
        return operationExpire;
    }

    public void setOperationExpire(int operationExpire) {
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

    public int getProcessingTimeoutMillis() {
        return processingTimeoutMillis;
    }

    public void setProcessingTimeoutMillis(int processingTimeoutMillis) {
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

    public int getDispatchingTimeoutMillis() {
        return dispatchingTimeoutMillis;
    }

    public void setDispatchingTimeoutMillis(int dispatchingTimeoutMillis) {
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

    public int getCasExpire() {
        return casExpire;
    }

    public void setCasExpire(int casExpire) {
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

    public int getMaxQueueDepth() {
        return maxQueueDepth;
    }

    public void setMaxQueueDepth(int maxQueueDepth) {
        this.maxQueueDepth = maxQueueDepth;
    }

    public int getMaxPreQueueDepth() {
        return maxPreQueueDepth;
    }

    public void setMaxPreQueueDepth(int maxPreQueueDepth) {
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

    public String getRedisPassword() {
        return redisPassword;
    }

    public void setRedisPassword(String redisPassword) {
        this.redisPassword = redisPassword;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public String[] getRedisNodes() {
        return redisNodes;
    }

    public void setRedisNodes(String[] redisNodes) {
        this.redisNodes = redisNodes;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public boolean isCacheCas() {
        return cacheCas;
    }

    public void setCacheCas(boolean cacheCas) {
        this.cacheCas = cacheCas;
    }

    @Override
    public String toString() {
        return "Backplane{" +
                "type='" + type + '\'' +
                ", redisUri='" + redisUri + '\'' +
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
                ", redisPassword='" + redisPassword + '\'' +
                ", timeout=" + timeout +
                ", redisNodes=" + Arrays.toString(redisNodes) +
                ", maxAttempts=" + maxAttempts +
                ", cacheCas=" + cacheCas +
                '}';
    }
}
