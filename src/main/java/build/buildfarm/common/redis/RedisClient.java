// Copyright 2020 The Bazel Authors. All rights reserved.
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

import io.grpc.Status;
import io.grpc.Status.Code;
import io.prometheus.client.Counter;
import java.io.Closeable;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Level;
import lombok.extern.java.Log;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.exceptions.JedisClusterMaxAttemptsException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.exceptions.JedisNoReachableClusterNodeException;

/**
 * @class RedisClient
 * @brief Responsible for making calls to redis.
 */
@Log
public class RedisClient implements Closeable {
  // Metrics to detect any kind of redis failures.
  // Often due to network issues are the redis cluster going down.
  private static final Counter redisErrorCounter =
      Counter.build().name("redis_client_error").help("Count of redis client failures").register();
  private static final Counter redisClientRebuildErrorCounter =
      Counter.build()
          .name("redis_client_rebuild_error")
          .help("Count of failures rebuilding redis client")
          .register();

  // Settings for attempting to reconnect the client on redis failures.
  private int reconnectClientAttempts = 0;
  private int reconnectClientWaitDurationMs = 0;

  private static final String MISCONF_RESPONSE = "MISCONF";

  @FunctionalInterface
  public interface JedisContext<T> {
    T run(JedisCluster jedis) throws JedisException;
  }

  @FunctionalInterface
  public interface JedisInterruptibleContext<T> {
    T run(JedisCluster jedis) throws InterruptedException, JedisException;
  }

  private static class JedisMisconfigurationException extends JedisDataException {
    public JedisMisconfigurationException(final String message) {
      super(message);
    }

    public JedisMisconfigurationException(final Throwable cause) {
      super(cause);
    }

    public JedisMisconfigurationException(final String message, final Throwable cause) {
      super(message, cause);
    }
  }

  // We store the factory in case we want to re-create the jedis client.
  private Supplier<JedisCluster> jedisClusterFactory;

  // The jedis client.
  private JedisCluster jedis;

  private boolean closed = false;

  public RedisClient(JedisCluster jedis) {
    this.jedis = jedis;
  }

  public RedisClient(
      Supplier<JedisCluster> jedisClusterFactory,
      int reconnectClientAttempts,
      int reconnectClientWaitDurationMs) {
    try {
      this.jedis = jedisClusterFactory.get();
    } catch (Exception e) {
      log.log(Level.SEVERE, "Unable to establish redis client: " + e.toString());
    }
    this.jedisClusterFactory = jedisClusterFactory;
    this.reconnectClientAttempts = reconnectClientAttempts;
    this.reconnectClientWaitDurationMs = reconnectClientWaitDurationMs;
  }

  @Override
  public synchronized void close() {
    closed = true;
  }

  public synchronized boolean isClosed() {
    return closed;
  }

  private synchronized void throwIfClosed() throws IOException {
    if (closed) {
      throw new IOException(
          Status.UNAVAILABLE.withDescription("client is closed").asRuntimeException());
    }
  }

  public void run(Consumer<JedisCluster> withJedis) throws IOException {
    call(
        (JedisContext<Void>)
            jedis -> {
              withJedis.accept(jedis);
              return null;
            });
  }

  public <T> T blockingCall(JedisInterruptibleContext<T> withJedis)
      throws IOException, InterruptedException {
    return defaultBlockingCall(withJedis);
  }

  private <T> T defaultBlockingCall(JedisInterruptibleContext<T> withJedis)
      throws IOException, InterruptedException {
    AtomicReference<InterruptedException> interruption = new AtomicReference<>(null);
    T result =
        call(
            jedis -> {
              try {
                return withJedis.run(jedis);
              } catch (InterruptedException e) {
                interruption.set(e);
                return null;
              }
            });
    InterruptedException e = interruption.get();
    if (e != null) {
      throw e;
    }
    return result;
  }

  public <T> T call(JedisContext<T> withJedis) throws IOException {
    return callImpl(withJedis);
  }

  private <T> T callImpl(JedisContext<T> withJedis) throws IOException {
    // Capture all redis problems at the client level.
    // Try to re-establish the client and log all issues.
    // This will block the overall thread until redis can be connected to.
    // It may be a useful strategy for gaining stability on a poorly performing network,
    // or a redis cluster that goes down.  For example, if you need to perform redis maintance on a
    // production cluster and do not want to affect build clients.
    for (int i = 0; i < reconnectClientAttempts; ++i) {
      try {
        return defaultCall(withJedis);
      } catch (Exception redisException) {
        // Record redis failure.
        redisErrorCounter.inc();
        log.log(
            Level.SEVERE,
            String.format(
                "Failure in RedisClient::callImpl (attempt %d of %d)",
                i + 1, reconnectClientAttempts));
        log.log(Level.SEVERE, redisException.toString());

        // Wait before restablishing the client and trying again.
        try {
          Thread.sleep(reconnectClientWaitDurationMs);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        rebuildJedisCluser();
      }
    }

    // If all attempts to re-establish the redis client has failed,
    // perform the redis command and allow the exception to be thrown.
    return defaultCall(withJedis);
  }

  private void rebuildJedisCluser() {
    try {
      log.log(Level.SEVERE, "Rebuilding redis client");
      jedis = jedisClusterFactory.get();
    } catch (Exception e) {
      redisClientRebuildErrorCounter.inc();
      log.log(Level.SEVERE, "Failed to rebuild redis client");
      log.log(Level.SEVERE, e.toString());
    }
  }

  private <T> T defaultCall(JedisContext<T> withJedis) throws IOException {
    throwIfClosed();
    try {
      try {
        return withJedis.run(jedis);
      } catch (JedisDataException e) {
        if (e.getMessage().startsWith(MISCONF_RESPONSE)) {
          throw new JedisMisconfigurationException(e.getMessage());
        }
        throw e;
      }
    } catch (JedisMisconfigurationException | JedisNoReachableClusterNodeException e) {
      // In regards to a Jedis misconfiguration,
      // the backplane is configured not to accept writes currently
      // as a result of an error. The error is meant to indicate
      // that substantial resources were unavailable.
      // we must throw an IOException which indicates as much
      // this looks simply to me like a good opportunity to use UNAVAILABLE
      // we are technically not at RESOURCE_EXHAUSTED, this is a
      // persistent state which can exist long past the error
      throw new IOException(Status.UNAVAILABLE.withCause(e).asRuntimeException());
    } catch (JedisConnectionException e) {
      if ((e.getMessage() != null && e.getMessage().equals("Unexpected end of stream."))
          || e.getCause() instanceof ConnectException) {
        throw new IOException(Status.UNAVAILABLE.withCause(e).asRuntimeException());
      }
      Throwable cause = e;
      Status status = Status.UNKNOWN;
      while (status.getCode() == Code.UNKNOWN && cause != null) {
        String message = cause.getMessage() == null ? "" : cause.getMessage();
        if ((cause instanceof SocketException && cause.getMessage().equals("Connection reset"))
            || cause instanceof ConnectException
            || message.equals("Unexpected end of stream.")) {
          status = Status.UNAVAILABLE;
        } else if (cause instanceof SocketTimeoutException) {
          status = Status.DEADLINE_EXCEEDED;
        } else if (cause instanceof IOException) {
          throw (IOException) cause;
        } else {
          cause = cause.getCause();
        }
      }
      throw new IOException(status.withCause(cause == null ? e : cause).asRuntimeException());
    } catch (JedisClusterMaxAttemptsException e) {
      throw new IOException(Status.UNAVAILABLE.withCause(e.getCause()).asRuntimeException());
    }
  }
}
