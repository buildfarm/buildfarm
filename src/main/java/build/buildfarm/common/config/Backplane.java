package build.buildfarm.common.config;

import com.google.common.base.Strings;
import java.net.URI;
import javax.annotation.Nullable;
import lombok.Data;
import lombok.ToString;
import oshi.util.FileUtil;
import redis.clients.jedis.util.JedisURIHelper;

@Data
public class Backplane {
  public enum BACKPLANE_TYPE {
    SHARD
  }

  private BACKPLANE_TYPE type;
  private String redisUri;
  private int jedisPoolMaxTotal;
  private int jedisPoolMaxIdle;
  private int jedisPoolMinIdle;
  private long jedisTimeBetweenEvictionRunsMillis;
  private String workersHashName;
  private String workerChannel;
  private String actionCachePrefix;
  private int actionCacheExpire;
  private String actionBlacklistPrefix;
  private int actionBlacklistExpire;
  private String invocationBlacklistPrefix;
  private String operationPrefix;
  private String actionsPrefix;
  private int operationExpire;
  private int actionExecutionExpire;
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
  private String correlatedInvocationsIndexPrefix;
  private int maxCorrelatedInvocationsIndexTimeout;
  private String correlatedInvocationsPrefix;
  private int maxCorrelatedInvocationsTimeout;
  private String toolInvocationsPrefix;
  private int maxToolInvocationTimeout;
  private int maxQueueDepth;
  private int maxPreQueueDepth;
  private boolean priorityQueue;
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

  private int timeout;
  private String[] redisNodes = {};
  private int maxAttempts;
  private long priorityPollIntervalMillis;

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
