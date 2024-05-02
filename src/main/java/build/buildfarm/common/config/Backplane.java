package build.buildfarm.common.config;

import com.google.common.base.Strings;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.ToString;
import oshi.util.FileUtil;
import redis.clients.jedis.util.JedisURIHelper;

@Data
public class Backplane {
  public enum BACKPLANE_TYPE {
    SHARD
  }

  private BACKPLANE_TYPE type = BACKPLANE_TYPE.SHARD;
  private String redisUri;
  private int jedisPoolMaxTotal = 4000;
  private String workersHashName = "Workers";
  private String workerChannel = "WorkerChannel";
  private String actionCachePrefix = "ActionCache";
  private int actionCacheExpire = 2419200; // 4 Weeks
  private String actionBlacklistPrefix = "ActionBlacklist";
  private int actionBlacklistExpire = 3600; // 1 Hour;
  private String invocationBlacklistPrefix = "InvocationBlacklist";
  private String operationPrefix = "Operation";
  private int operationExpire = 604800; // 1 Week
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
  private int casExpire = 604800; // 1 Week
  private int maxInvocationIdTimeout = 604800;

  @Getter(AccessLevel.NONE)
  private boolean subscribeToBackplane = true; // deprecated

  @Getter(AccessLevel.NONE)
  private boolean runFailsafeOperation = true; // deprecated

  private int maxQueueDepth = 100000;
  private int maxPreQueueDepth = 1000000;
  private boolean priorityQueue = false;
  private Queue[] queues = {};
  private String redisCredentialFile;
  @ToString.Exclude // Do not log the password on start-up.
  private String redisPassword;

  /**
   * Path to a CA.pem for the redis TLS. If specified, ONLY this root CA will be used (it will not
   * be added to the defaults)
   */
  private String redisCertificateAuthorityFile;

  private int timeout = 10000;
  private String[] redisNodes = {};
  private int maxAttempts = 20;
  private boolean cacheCas = false;
  private long priorityPollIntervalMillis = 100;

  // These limited resources are shared across all workers.
  // An example would be a limited number of seats to a license server.
  private List<LimitedResource> resources = new ArrayList<>();

  /**
   * Look in several prioritized ways to get a Redis password:
   *
   * <ol>
   *   <li>the password in the Redis URI (wherever that came from)
   *   <li>The `redisPassword` from config YAML
   *   <li>the `redisCredentialFile`.
   * </ol>
   *
   * @return The redis password, or <c>null</c> if unset.
   */
  public @Nullable String getRedisPassword() {
    String r = getRedisUri();
    if (r == null) {
      return null;
    }
    URI redisProperUri = URI.create(r);
    if (!Strings.isNullOrEmpty(JedisURIHelper.getPassword(redisProperUri))) {
      return JedisURIHelper.getPassword(redisProperUri);
    }

    if (!Strings.isNullOrEmpty(redisCredentialFile)) {
      // Get the password from the config file.
      return FileUtil.getStringFromFile(redisCredentialFile);
    }

    return Strings.emptyToNull(redisPassword);
  }
}
