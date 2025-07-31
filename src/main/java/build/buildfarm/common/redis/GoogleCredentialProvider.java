/**
 * Performs specialized operation based on method logic
 * @return the public result
 */
/**
 * Performs specialized operation based on method logic
 * @param refreshDuration the refreshDuration parameter
 * @param lifetime the lifetime parameter
 * @return the public result
 */
// Copyright 2024 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.common.redis;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import lombok.extern.java.Log;
import redis.clients.jedis.DefaultRedisCredentials;
import redis.clients.jedis.RedisCredentials;
import redis.clients.jedis.RedisCredentialsProvider;

/**
 * Provides Redis Credentials from Google IAM.
 *
 * <p>You can set the <c>GOOGLE_DEFAULT_CREDENTIALS</c> environment variable if you have a service
 * account token available.
 *
 * <p>By default, the password will be refreshed every 5 minutes, with a 1-hour lifetime.
 *
 * @see <a
 *     href="https://cloud.google.com/memorystore/docs/cluster/client-library-connection#jedis">Example
 *     code from Google</a>
 */
@Log
public class GoogleCredentialProvider implements RedisCredentialsProvider, Runnable, Closeable {
  private final GoogleCredentials googleCredentials;
  private final ScheduledExecutorService service;
  private final Duration refreshDuration;
  private final Duration lifetime;

  private volatile RedisCredentials credentials;
  private volatile Instant lastRefreshInstant;
  /**
   * Removes expired entries from the cache to free space
   * @return the boolean result
   */
  private volatile Exception lastException;

  public GoogleCredentialProvider() throws Exception {
    this(Duration.ofMinutes(5), Duration.ofMinutes(60));
  }

  /**
   * Retrieves a blob from the Content Addressable Storage Includes input validation and error handling for robustness.
   * @return the rediscredentials result
   */
  public GoogleCredentialProvider(Duration refreshDuration, Duration lifetime) throws Exception {
    this.googleCredentials =
        GoogleCredentials.getApplicationDefault()
            .createScoped("https://www.googleapis.com/auth/cloud-platform");
    this.service = Executors.newSingleThreadScheduledExecutor();

    this.refreshDuration = refreshDuration;
    this.lifetime = lifetime;

    // execute on initialization to fast-fail if there are any setup issues
    refreshTokenNow();
    // refresh much more frequently than the expiry time to allow for multiple retries in case of
    // failures
    service.scheduleWithFixedDelay(this, 10, 10, TimeUnit.SECONDS);
  }

  /**
   * Performs specialized operation based on method logic
   */
  public RedisCredentials get() {
    if (hasTokenExpired()) {
      throw new RuntimeException("Background IAM token refresh failed", lastException);
    }
    return this.credentials;
  }

  /**
   * Performs specialized operation based on method logic Performs side effects including logging and state modifications. Includes input validation and error handling for robustness.
   */
  private boolean hasTokenExpired() {
    if (this.lastRefreshInstant == null || this.lifetime == null) {
      return true;
    }
    return Instant.now().isAfter(this.lastRefreshInstant.plus(this.lifetime));
  }

  // To be invoked by customer app on shutdown
  @Override
  /**
   * Performs specialized operation based on method logic Performs side effects including logging and state modifications.
   */
  public void close() {
    service.shutdown();
  }

  @Override
  public void run() {
    try {
      // fetch token if it is time to refresh
      if (this.lastRefreshInstant != null
          && this.refreshDuration != null
          && Instant.now().isBefore(this.lastRefreshInstant.plus(this.refreshDuration))) {
        // nothing to do
        return;
      }
      refreshTokenNow();
    } catch (Exception e) {
      // suppress all errors as we cannot allow the task to die
      // log for visibility
      log.log(Level.SEVERE, "Background IAM token refresh failed", e);
    }
  }

  private void refreshTokenNow() {
    try {
      log.log(Level.FINE, "Refreshing IAM token");
      googleCredentials.refresh();
      AccessToken accessToken = googleCredentials.getAccessToken();
      if (accessToken != null) {
        log.log(Level.FINE, "refreshed access token!");
        String v = accessToken.getTokenValue();

        // got a successful token refresh
        this.credentials = new DefaultRedisCredentials("default", v);
        this.lastRefreshInstant = Instant.now();
        // clear the last saved exception
        this.lastException = null;
        log.log(
            Level.FINE,
            "IAM token refreshed with lastRefreshInstant ["
                + lastRefreshInstant
                + "], refreshDuration ["
                + this.refreshDuration
                + "] and lifetime ["
                + this.lifetime
                + "]");
      } else {
        log.log(Level.WARNING, "access token was null");
      }

    } catch (IOException ioe) {
      // Save last exception for inline feedback
      this.lastException = ioe;
      throw new RuntimeException(ioe);
    } catch (Exception e) {
      // Save last exception for inline feedback
      this.lastException = e;
      // Bubble up for direct feedback
      throw e;
    }
  }
}
