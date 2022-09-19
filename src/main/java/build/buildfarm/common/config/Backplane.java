package build.buildfarm.common.config;

import com.google.common.base.Strings;
import java.util.List;
import lombok.Data;

@Data
public class Backplane {
  public enum BACKPLANE_TYPE {
    SHARD
  }

  private String workersHashName = "Workers";
  private String workerChannel = "WorkerChannel";
  private String actionCachePrefix = "ActionCache";
  private String actionBlacklistPrefix = "ActionBlacklist";
  private String invocationBlacklistPrefix = "InvocationBlacklist";
  private String operationPrefix = "Operation";
  private String preQueuedOperationsListName = "{Arrival}:PreQueuedOperations";
  private String processingListName = "{Arrival}:ProcessingOperations";
  private String processingPrefix = "Processing";
  private String queuedOperationsListName = "{Execution}:QueuedOperations";
  private String dispatchingPrefix = "Dispatching";
  private String dispatchedOperationsHashName = "DispatchedOperations";
  private String operationChannelPrefix = "OperationChannel";
  private String casPrefix = "ContentAddressableStorage";
  private BACKPLANE_TYPE type;
  private String redisUri;
  private int jedisPoolMaxTotal;
  private int actionCacheExpire;
  private int actionBlacklistExpire;
  private int operationExpire;
  private int processingTimeoutMillis;
  private int dispatchingTimeoutMillis;
  private int casExpire;
  private boolean subscribeToBackplane;
  private boolean runFailsafeOperation;
  private int maxQueueDepth;
  private int maxPreQueueDepth;
  private boolean priorityQueue;
  private List<Queue> queues;
  private String redisPassword;
  private int timeout;
  private List<String> redisNodes;
  private int maxAttempts;
  private boolean cacheCas;

  public String getRedisUri() {
    if (!Strings.isNullOrEmpty(redisUri)) {
      return redisUri;
    } else {
      return System.getenv("REDIS_URI");
    }
  }
}
