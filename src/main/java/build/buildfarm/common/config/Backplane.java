package build.buildfarm.common.config;

import com.google.common.base.Strings;
import lombok.Data;

@Data
public class Backplane {
  public enum BACKPLANE_TYPE {
    SHARD
  }

  private BACKPLANE_TYPE type;
  private String redisUri;
  private int jedisPoolMaxTotal;
  private String workersHashName;
  private String workerChannel;
  private String actionCachePrefix;
  private int actionCacheExpire;
  private String actionBlacklistPrefix;
  private int actionBlacklistExpire;
  private String invocationBlacklistPrefix;
  private String operationPrefix;
  private int operationExpire;
  private String preQueuedOperationsListName;
  private String processingListName;
  private String processingPrefix;
  private int processingTimeoutMillis;
  private String queuedOperationsListName;
  private String dispatchingPrefix;
  private int dispatchingTimeoutMillis;
  private String dispatchedOperationsHashName;
  private String operationChannelPrefix;
  private String casPrefix;
  private int casExpire;
  private boolean subscribeToBackplane;
  private boolean runFailsafeOperation;
  private int maxQueueDepth;
  private int maxPreQueueDepth;
  private boolean priorityQueue;
  private Queue[] queues;
  private String redisPassword;
  private int timeout;
  private String[] redisNodes;
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
