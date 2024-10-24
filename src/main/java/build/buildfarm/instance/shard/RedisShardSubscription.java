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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Level;
import lombok.extern.java.Log;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.exceptions.JedisException;

@Log
class RedisShardSubscription implements Runnable {
  private final RedisShardSubscriber subscriber;
  private final InterruptingRunnable onUnsubscribe;
  private final Consumer<UnifiedJedis> onReset;
  private final Supplier<List<String>> subscriptions;
  private final RedisClient client;

  private enum SubscriptionAction {
    STOP,
    START_SUBSCRIBE,
    END_SUBSCRIPTION
  }

  private enum SubscriptionState {
    NOT_SUBSCRIBED,
    SUBSCRIBING,
    SUBSCRIBED,
    STOPPED_BUT_SUBSCRIBED,
    FULLY_STOPPED
  }

  private final AtomicReference<SubscriptionState> subscriptionState =
      new AtomicReference<>(SubscriptionState.NOT_SUBSCRIBED);
  private static final long DEFAULT_STOP_TIMEOUT = 1000;

  RedisShardSubscription(
      RedisShardSubscriber subscriber,
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
    SubscriptionState currentState = subscriptionState.get();
    switch (update) {
      case STOP:
        if (currentState == SubscriptionState.NOT_SUBSCRIBED) {
          subscriptionState.set(SubscriptionState.FULLY_STOPPED);
        } else if (currentState == SubscriptionState.SUBSCRIBING
            || currentState == SubscriptionState.SUBSCRIBED) {
          subscriptionState.set(SubscriptionState.STOPPED_BUT_SUBSCRIBED);
        }
        break;
      case START_SUBSCRIBE:
        if (currentState != SubscriptionState.STOPPED_BUT_SUBSCRIBED
            && currentState != SubscriptionState.FULLY_STOPPED) {
          subscriptionState.set(SubscriptionState.SUBSCRIBING);
        }
        break;
      case END_SUBSCRIPTION:
        if (currentState == SubscriptionState.STOPPED_BUT_SUBSCRIBED
            || currentState == SubscriptionState.FULLY_STOPPED) {
          subscriptionState.set(SubscriptionState.FULLY_STOPPED);
        } else {
          subscriptionState.set(SubscriptionState.NOT_SUBSCRIBED);
        }
        break;
    }
  }

  private void subscribe(UnifiedJedis jedis, boolean isReset) {
    if (isReset) {
      onReset.accept(jedis);
    }
    manageState(SubscriptionAction.START_SUBSCRIBE);
    if (subscriptionState.get() == SubscriptionState.SUBSCRIBING) {
      jedis.subscribe(subscriber, subscriptions.get().toArray(new String[0]));
      manageState(SubscriptionAction.END_SUBSCRIPTION);
    } else {
      log.log(
          Level.SEVERE,
          "Cannot subscribe, RedisShardSubscription is in 'stopped' state "
              + subscriptionState.get().name());
      manageState(SubscriptionAction.END_SUBSCRIPTION);
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
          manageState(SubscriptionAction.END_SUBSCRIPTION);
          /* ignore */
          break;
        default:
          throw e;
      }
    }
  }

  private void mainLoop() throws IOException {
    boolean first = true;
    while (subscriptionState.get() != SubscriptionState.STOPPED_BUT_SUBSCRIBED
        && subscriptionState.get() != SubscriptionState.FULLY_STOPPED) {
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
      if (subscriptionState.get() == SubscriptionState.STOPPED_BUT_SUBSCRIBED) {
        try {
          subscriber.checkIfSubscribed(timeoutMillis);
        } catch (TimeoutException e) {
          throw new UnsubscribeTimeoutException(
              "Call to stop subscription timed out while waiting for"
                  + " RedisShardSubscriber::subscribe to complete. Subscriber is still active.");
        } catch (InterruptedException | ExecutionException e) {
          throw new UnsubscribeException(
              "Call to stop subscription was interrupted or errored before unsubscribing. "
                  + "Subscriber is still active. \n"
                  + "Exception message: "
                  + e.getMessage());
        }
      }
      subscriber.unsubscribe();
    } catch (JedisException e) {
      // If stop() is called without an established connection, log and throw the exception
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
    stop(DEFAULT_STOP_TIMEOUT);
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
