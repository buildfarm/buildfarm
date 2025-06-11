package build.buildfarm.common.config;

import com.google.common.base.Strings;
import java.net.URI;
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
  private int jedisPoolMaxTotal = 200;
  private int jedisPoolMaxIdle = 8;
  private int jedisPoolMinIdle = 0;
  private long jedisTimeBetweenEvictionRunsMillis = 30000L;
  private String workersHashName = "Workers";
  private String workerChannel = "WorkerChannel";
  private String actionCachePrefix = "ActionCache";
  private int actionCacheExpire = 2419200; // 4 Weeks
  private String actionBlacklistPrefix = "ActionBlacklist";
  private int actionBlacklistExpire = 3600; // 1 Hour;
  private String invocationBlacklistPrefix = "InvocationBlacklist";
  private String operationPrefix = "Operation";
  private String actionsPrefix = "Action";
  private int operationExpire = 604800; // 1 Week
  private int actionExecutionExpire = 21600; // 6 Hours
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
  private String correlatedInvocationsIndexPrefix = "CorrelatedInvocationsIndex";
  private int maxCorrelatedInvocationsIndexTimeout = 3 * 24 * 60 * 60; // 3 Days
  private String correlatedInvocationsPrefix = "CorrelatedInvocations";
  private int maxCorrelatedInvocationsTimeout = 7 * 24 * 60 * 60; // 1 Week
  private String toolInvocationsPrefix = "ToolInvocation";
  private int maxToolInvocationTimeout = 604800;

  @Getter(AccessLevel.NONE)
  private boolean subscribeToBackplane = true; // deprecated

  @Getter(AccessLevel.NONE)
  private boolean runFailsafeOperation = true; // deprecated

  private int maxQueueDepth = 100000;
  private int maxPreQueueDepth = 1000000;
  private boolean priorityQueue = false;
  private Queue[] queues = {};
  private String redisCredentialFile;
  private String redisUsername;
  @ToString.Exclude // Do not log the password on start-up.
  private String redisPassword;

  /**
   * Path to a CA.pem for the redis TLS. If specified, ONLY this root CA will be used (it will not
   * be added to the defaults)
   */
  private String redisCertificateAuthorityFile;

  /**
   * Use Google Application Default Credentials to authenticate to Redis.
   *
   * <p>Setting GOOGLE_DEFAULT_CREDENTIALS env var can help Google credential provider find your
   * service account.
   *
   * <p>If this is set, the `redisPassword` will be ignored.
   */
  private boolean redisAuthWithGoogleCredentials;

  private int timeout = 10000; // Milliseconds
  private String[] redisNodes = {};
  private int maxAttempts = 20;
  private long priorityPollIntervalMillis = 100;

  /**
   * This function is used to print the URI in logs.
   *
   * @return The redis URI but the password will be hidden, or <c>null</c> if unset.
   */
  public @Nullable String getRedisUriMasked() {
    String uri = getRedisUri();
    if (uri == null) {
      return null;
    }
    try {
      URI redisProperUri = URI.create(uri);
      String password = JedisURIHelper.getPassword(redisProperUri);
      if (Strings.isNullOrEmpty(password)) {
        return uri;
      }
      return uri.replace(password, "<HIDDEN>");
    } catch (ArrayIndexOutOfBoundsException e) {
      // JedisURIHelper.getPassword did not find the password (e.g. only username in
      // uri.getUserInfo)
      return uri;
    }
  }

  /**
   * Look in several prioritized ways to get a Redis username:
   *
   * <ol>
   *   <li>the password in the Redis URI (wherever that came from)
   *   <li>The `redisUsername` from config YAML
   * </ol>
   *
   * @return The redis username, or <c>null</c> if unset.
   */
  public @Nullable String getRedisUsername() {
    String r = getRedisUri();
    if (r != null) {
      URI redisProperUri = URI.create(r);
      String username = JedisURIHelper.getUser(redisProperUri);
      if (!Strings.isNullOrEmpty(username)) {
        return username;
      }
    }

    return Strings.emptyToNull(redisUsername);
  }

  /**
   * Look in several prioritized ways to get a Redis password:
   *
   * <ol>
   *   <li>the password in the Redis URI (wherever that came from)
   *   <li>the `redisCredentialFile`.
   *   <li>The `redisPassword` from config YAML
   * </ol>
   *
   * @return The redis password, or <c>null</c> if unset.
   */
  public @Nullable String getRedisPassword() {
    String r = getRedisUri();
    if (r == null) {
      return null;
    }
    try {
      URI redisProperUri = URI.create(r);
      if (!Strings.isNullOrEmpty(JedisURIHelper.getPassword(redisProperUri))) {
        return JedisURIHelper.getPassword(redisProperUri);
      }
    } catch (ArrayIndexOutOfBoundsException e) {
      // JedisURIHelper.getPassword did not find the password (e.g. only username in
      // uri.getUserInfo)
    }

    if (!Strings.isNullOrEmpty(redisCredentialFile)) {
      // Get the password from the config file.
      return FileUtil.getStringFromFile(redisCredentialFile);
    }

    return Strings.emptyToNull(redisPassword);
  }
}
