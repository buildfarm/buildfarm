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

import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;

class RedisShardSubscription implements Runnable {
  private final String channel;
  private final JedisPubSub subscriber;
  private final Runnable onUnsubscribe;
  private final Consumer<Jedis> onReset;
  private final Supplier<Jedis> jedisFactory;
  private boolean done = false;

  RedisShardSubscription(
      String channel,
      JedisPubSub subscriber,
      Runnable onUnsubscribe,
      Consumer<Jedis> onReset,
      Supplier<Jedis> jedisFactory) {
    this.channel = channel;
    this.subscriber = subscriber;
    this.onUnsubscribe = onUnsubscribe;
    this.onReset = onReset;
    this.jedisFactory = jedisFactory;
  }

  private void subscribe(Jedis jedis, boolean isReset) throws IOException {
    try {
      if (isReset) {
        onReset.accept(jedis);
      }
      jedis.subscribe(subscriber, channel);
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
    System.err.println("RedisShardSubscription(" + channel + ") Unexpected subscribe return, reconnecting...");
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
      e.printStackTrace();
      System.err.println("RedisShardSubscription(" + channel + ") Calling onUnsubscribe...");
      onUnsubscribe.run();
    }
  }
}
