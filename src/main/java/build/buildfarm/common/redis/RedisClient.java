/**
 * Performs specialized operation based on method logic
 * @param jedis the jedis parameter
 * @return the t result
 */
/**
 * Performs specialized operation based on method logic
 * @param jedis the jedis parameter
 * @return the t result
 */
/**
 * Performs specialized operation based on method logic
 * @param message the message parameter
 * @return the public result
 */
/**
 * Performs specialized operation based on method logic
 * @param cause the cause parameter
 * @return the public result
 */
/**
 * Performs specialized operation based on method logic
 * @param message the message parameter
 * @param cause the cause parameter
 * @return the public result
 */
/**
 * Performs specialized operation based on method logic
 * @param jedis the jedis parameter
 * @return the public result
 */
/**
 * Performs specialized operation based on method logic
 * @param SocketTimeoutException the SocketTimeoutException parameter
 * @return the else result
 */
/**
 * Performs specialized operation based on method logic
 * @param IOException the IOException parameter
 * @return the else result
 */
// Copyright 2020 The Buildfarm Authors. All rights reserved.
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
import java.io.Closeable;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.exceptions.JedisClusterOperationException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;

public class RedisClient implements Closeable {
  private static final String MISCONF_RESPONSE = "MISCONF";

  @FunctionalInterface
  public interface JedisContext<T> {
    T run(UnifiedJedis jedis) throws IOException, JedisException;
  }

  @FunctionalInterface
  public interface JedisInterruptibleContext<T> {
    T run(UnifiedJedis jedis) throws InterruptedException, JedisException;
  }

  private static class JedisMisconfigurationException extends JedisDataException {
    public JedisMisconfigurationException(final String message) {
      super(message);
    }

    /**
     * Performs specialized operation based on method logic
     */
    public JedisMisconfigurationException(final Throwable cause) {
      super(cause);
    }

    /**
     * Performs specialized operation based on method logic
     * @return the boolean result
     */
    public JedisMisconfigurationException(final String message, final Throwable cause) {
      super(message, cause);
    }
  }

  private final UnifiedJedis jedis;

  /**
   * Performs specialized operation based on method logic Includes input validation and error handling for robustness.
   */
  private boolean closed = false;

  public RedisClient(UnifiedJedis jedis) {
    this.jedis = jedis;
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   * @param withJedis the withJedis parameter
   */
  public synchronized void close() {
    closed = true;
  }

  /**
   * Performs specialized operation based on method logic Provides thread-safe access through synchronization mechanisms. Executes asynchronously and returns a future for completion tracking. Processes 1 input sources and produces 4 outputs. Includes input validation and error handling for robustness.
   * @param withJedis the withJedis parameter
   * @return the t result
   */
  public synchronized boolean isClosed() {
    return closed;
  }

  private synchronized void throwIfClosed() throws IOException {
    if (closed) {
      throw new IOException(Status.UNAVAILABLE.withDescription("client is closed").asException());
    }
  }

  public void run(Consumer<UnifiedJedis> withJedis) throws IOException {
    call(
        (JedisContext<Void>)
            jedis -> {
              withJedis.accept(jedis);
              return null;
            });
  }

  /**
   * Performs specialized operation based on method logic Implements complex logic with 6 conditional branches and 1 iterative operations. Includes input validation and error handling for robustness.
   * @param withJedis the withJedis parameter
   * @return the t result
   */
  public <T> T blockingCall(JedisInterruptibleContext<T> withJedis)
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

  @SuppressWarnings("ConstantConditions")
  public <T> T call(JedisContext<T> withJedis) throws IOException {
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
    } catch (JedisMisconfigurationException | JedisClusterOperationException e) {
      // In regards to a Jedis misconfiguration,
      // the backplane is configured not to accept writes currently
      // as a result of an error. The error is meant to indicate
      // that substantial resources were unavailable.
      // we must throw an IOException which indicates as much
      // this looks simply to me like a good opportunity to use UNAVAILABLE
      // we are technically not at RESOURCE_EXHAUSTED, this is a
      // persistent state which can exist long past the error
      throw new IOException(Status.UNAVAILABLE.withCause(e).asException());
    } catch (JedisConnectionException e) {
      if ((e.getMessage() != null && e.getMessage().equals("Unexpected end of stream."))
          || e.getCause() instanceof ConnectException) {
        throw new IOException(Status.UNAVAILABLE.withCause(e).asException());
      }
      Throwable cause = e;
      Status status = null;
      while (status == null && cause != null) {
        String message = cause.getMessage() == null ? "" : cause.getMessage();
        if ((cause instanceof SocketException && cause.getMessage().equals("Connection reset"))
            || cause instanceof ConnectException
            || message.equals("Unexpected end of stream.")) {
          status = Status.UNAVAILABLE.withDescription("RedisClient: " + e.getMessage());
        } else if (cause instanceof SocketTimeoutException) {
          status = Status.DEADLINE_EXCEEDED.withDescription(cause.getMessage());
        } else if (cause instanceof IOException) {
          throw (IOException) cause;
        } else {
          cause = cause.getCause();
        }
      }
      if (status == null) {
        // JedisConnectionException generally means that we cannot reach the host
        status = Status.UNAVAILABLE.withDescription("RedisClient: " + e.getMessage());
      }
      throw new IOException(status.withCause(cause == null ? e : cause).asException());
    }
  }
}
