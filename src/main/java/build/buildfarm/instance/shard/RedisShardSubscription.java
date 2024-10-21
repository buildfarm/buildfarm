// Copyright 2018 The Buildfarm Authors. All rights reserved.
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

import static build.buildfarm.common.io.Utils.formatIOError;

import build.buildfarm.common.function.InterruptingRunnable;
import build.buildfarm.common.redis.RedisClient;
import io.grpc.Status;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Level;
import lombok.extern.java.Log;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.exceptions.JedisException;

@Log
class RedisShardSubscription implements Runnable {
  private final JedisPubSub subscriber;
  private final InterruptingRunnable onUnsubscribe;
  private final Consumer<UnifiedJedis> onReset;
  private final Supplier<List<String>> subscriptions;
  private final RedisClient client;
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private final AtomicBoolean attemptingSubscription = new AtomicBoolean(false);

  public static final int SUBSCRIBE_POLL_PERIOD = 1;

  private enum SubscriptionAction {
    STOP,
    START_SUBSCRIBE,
    COMPLETE_SUBSCRIBE
  }

  RedisShardSubscription(
      JedisPubSub subscriber,
      InterruptingRunnable onUnsubscribe,
      Consumer<UnifiedJedis> onReset,
      Supplier<List<String>> subscriptions,
      RedisClient client) {
    this.subscriber = subscriber;
    this.onUnsubscribe = onUnsubscribe;
    this.onReset = onReset;
    this.subscriptions = subscriptions;
    this.client = client;
  }

  private synchronized void manageState(SubscriptionAction update) {
    switch (update) {
      case STOP:
        stopped.set(true);
        break;
      case START_SUBSCRIBE:
        if (!stopped.get()) {
          attemptingSubscription.set(true);
        }
        break;
      case COMPLETE_SUBSCRIBE:
        attemptingSubscription.set(false);
        break;
    }
  }

  private void subscribe(UnifiedJedis jedis, boolean isReset) {
    if (isReset) {
      onReset.accept(jedis);
    }
    manageState(SubscriptionAction.START_SUBSCRIBE);
    if (attemptingSubscription.get()) {
      jedis.subscribe(subscriber, subscriptions.get().toArray(new String[0]));
      manageState(SubscriptionAction.COMPLETE_SUBSCRIBE);
    } else {
      log.log(Level.SEVERE, "Cannot subscribe, RedisShardSubscription is in 'stopped' state");
    }
  }

  private void iterate(boolean isReset) throws IOException {
    try {
      client.run(jedis -> subscribe(jedis, isReset));
    } catch (IOException e) {
      Status status = Status.fromThrowable(e);
      switch (status.getCode()) {
        case DEADLINE_EXCEEDED:
        case UNAVAILABLE:
          log.log(Level.WARNING, "failed to subscribe", formatIOError(e));
          /* ignore */
          break;
        default:
          throw e;
      }
    }
  }

  private void mainLoop() throws IOException {
    boolean first = true;
    while (!stopped.get()) {
      if (!first) {
        log.log(Level.SEVERE, "unexpected subscribe return, reconnecting...");
      }
      iterate(!first);
      first = false;
    }
  }

  public void stop(long timeoutMillis) {
    manageState(SubscriptionAction.STOP);
    try {
      long startTimeMillis = System.currentTimeMillis();
      while (attemptingSubscription.get() && !subscriber.isSubscribed()) {
        try {
          TimeUnit.MILLISECONDS.sleep(SUBSCRIBE_POLL_PERIOD);
        } catch (InterruptedException intEx) {
          log.log(
              Level.SEVERE,
              "Call to stop subscription was interrupted before unsubscribing. "
                  + "JedisPubSub subscriber is still active");
        }

        if (System.currentTimeMillis() - startTimeMillis > timeoutMillis) {
          throw new UnsubscribeTimeoutException(
              "Call to stop subscription timed out while waiting for JedisPubSub::subscribe to"
                  + " complete. Subscriber is still active.");
        }
      }
      subscriber.unsubscribe();
    } catch (JedisException e) {
      // If stop() is called before a connection is established, log and throw the exception
      if (e.getMessage().endsWith(" is not connected to a Connection.")) {
        log.log(
            Level.SEVERE,
            "RedisShardSubscription::stop called but no connection is established. "
                + "Subscription is now in 'Stopped' state and cannot subscribe.");
      }
      throw e;
    }
  }

  public void stop() {
    stop(1000);
  }

  @Override
  public void run() {
    try {
      mainLoop();
    } catch (Exception e) {
      log.log(Level.SEVERE, "RedisShardSubscription: Calling onUnsubscribe...", e);
      try {
        onUnsubscribe.runInterruptibly();
      } catch (InterruptedException intEx) {
        Thread.currentThread().interrupt();
      }
    } finally {
      manageState(SubscriptionAction.STOP);
    }
  }
}
