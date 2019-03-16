// Copyright 2018 The Bazel Authors. All rights reserved.
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

package build.buildfarm.instance.shard;

import static java.util.logging.Level.INFO;

import build.buildfarm.common.function.InterruptingRunnable;
import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;

class RedisShardSubscription implements Runnable {
  private final static Logger logger = Logger.getLogger(RedisShardSubscription.class.getName());

  private final JedisPubSub subscriber;
  private final InterruptingRunnable onUnsubscribe;
  private final Consumer<Jedis> onReset;
  private final Supplier<List<String>> subscriptions;
  private final Supplier<Jedis> jedisFactory;
  private boolean done = false;

  RedisShardSubscription(
      JedisPubSub subscriber,
      InterruptingRunnable onUnsubscribe,
      Consumer<Jedis> onReset,
      Supplier<List<String>> subscriptions,
      Supplier<Jedis> jedisFactory) {
    this.subscriber = subscriber;
    this.onUnsubscribe = onUnsubscribe;
    this.onReset = onReset;
    this.subscriptions = subscriptions;
    this.jedisFactory = jedisFactory;
  }

  public JedisPubSub getSubscriber() {
    return subscriber;
  }

  private void subscribe(Jedis jedis, boolean isReset) throws IOException {
    try {
      if (isReset) {
        onReset.accept(jedis);
      }
      jedis.subscribe(subscriber, subscriptions.get().toArray(new String[0]));
    } catch (JedisConnectionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw e;
    }
  }

  private void iterate(boolean isReset) throws IOException {
    try (Jedis jedis = jedisFactory.get()) {
      subscribe(jedis, isReset);
    } catch (SocketTimeoutException e) {
      // ignore
    } catch (SocketException e) {
      if (!e.getMessage().equals("Connection reset")) {
        throw e;
      }
    } catch (JedisConnectionException e) {
      if (!e.getMessage().equals("Unexpected end of stream.")) {
        throw e;
      }
    }
    logger.warning("RedisShardSubscription: Unexpected subscribe return, reconnecting...");
  }

  private void mainLoop() throws IOException {
    boolean first = true;
    while (!isDone()) {
      iterate(!first);
      first = false;
    }
  }

  private boolean isDone() {
    return done;
  }

  @Override
  public void run() {
    try {
      mainLoop();
    } catch (Exception e) {
      logger.log(INFO, "RedisShardSubscription: Calling onUnsubscribe...", e);
      try {
        onUnsubscribe.runInterruptibly();
      } catch (InterruptedException intEx) {
      }
    }
  }
}
