// Copyright 2017 The Bazel Authors. All rights reserved.
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

import static java.lang.String.format;

import build.buildfarm.instance.WatchFuture;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.longrunning.Operation;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import redis.clients.jedis.Client;
import redis.clients.jedis.JedisPubSub;

abstract class OperationSubscriber extends JedisPubSub {
  private static final Logger logger = Logger.getLogger(OperationSubscriber.class.getName());

  abstract static class TimedWatchFuture extends WatchFuture {
    private final TimedWatcher watcher;

    TimedWatchFuture(TimedWatcher watcher) {
      super(watcher::observe);
      this.watcher = watcher;
    }

    TimedWatcher getWatcher() {
      return watcher;
    }

    void complete() {
      super.set(null);
    }
  }

  private final ListMultimap<String, TimedWatchFuture> watchers;
  private final Executor executor;

  OperationSubscriber(
      ListMultimap<String, TimedWatchFuture> watchers,
      Executor executor) {
    this.watchers = watchers;
    this.executor = executor;
  }

  public List<String> watchedOperationChannels() {
    synchronized (watchers) {
      return ImmutableList.copyOf(watchers.keySet());
    }
  }

  public List<String> expiredWatchedOperationChannels(Instant now) {
    ImmutableList.Builder<String> builder = ImmutableList.builder();
    synchronized (watchers) {
      for (String channel : watchers.keySet()) {
        for (TimedWatchFuture watchFuture : watchers.get(channel)) {
          if (watchFuture.getWatcher().isExpiredAt(now)) {
            builder.add(channel);
            break;
          }
        }
      }
    }
    return builder.build();
  }

  // synchronizing on these because the client has been observed to
  // cause protocol desynchronization for multiple concurrent calls
  @Override
  public synchronized void unsubscribe() {
    if (isSubscribed()) {
      super.unsubscribe();
    }
  }

  @Override
  public synchronized void unsubscribe(String... channels) {
    super.unsubscribe(channels);
  }

  @Override
  public synchronized void subscribe(String... channels) {
    super.subscribe(channels);
  }

  @Override
  public synchronized void psubscribe(String... patterns) {
    super.psubscribe(patterns);
  }

  @Override
  public synchronized void punsubscribe() {
    super.punsubscribe();
  }

  @Override
  public synchronized void punsubscribe(String... patterns) {
    super.punsubscribe(patterns);
  }

  @Override
  public synchronized void ping() {
    super.ping();
  }

  public ListenableFuture<Void> watch(String channel, TimedWatcher watcher) {
    TimedWatchFuture watchFuture = new TimedWatchFuture(watcher) {
      @Override
      public void unwatch() {
        OperationSubscriber.this.unwatch(channel, this);
      }
    };
    boolean hasSubscribed;
    synchronized (watchers) {
      // use prefix
      hasSubscribed = watchers.containsKey(channel);
      watchers.put(channel, watchFuture);
      if (!hasSubscribed) {
        subscribe(channel);
      }
    }
    return watchFuture;
  }

  public void unwatch(String channel, TimedWatchFuture watchFuture) {
    synchronized (watchers) {
      if (watchers.remove(channel, watchFuture) &&
          !watchers.containsKey(channel)) {
        unsubscribe(channel);
      }
    }
  }

  public void resetWatchers(String channel, Instant expiresAt) {
    List<TimedWatchFuture> operationWatchers = watchers.get(channel);
    synchronized (watchers) {
      for (TimedWatchFuture watchFuture : operationWatchers) {
        watchFuture.getWatcher().reset(expiresAt);
      }
    }
  }

  private void terminateExpiredWatchers(String channel, Instant now) {
    onOperation(
        channel,
        /* operation=*/ null,
        (watcher) -> {
          boolean expired = watcher.isExpiredAt(now);
          if (expired) {
            logger.severe(format("Terminating expired watcher of %s because: %s >= %s", channel, now, watcher.getExpiresAt()));
          }
          return expired;
        },
        /* expiresAt=*/ null);
  }

  public void onOperation(String channel, Operation operation, Instant expiresAt) {
    onOperation(channel, operation, (watcher) -> true, expiresAt);
  }

  private void onOperation(
      String channel,
      @Nullable Operation operation,
      Predicate<TimedWatcher> shouldObserve,
      @Nullable Instant expiresAt) {
    List<TimedWatchFuture> operationWatchers = watchers.get(channel);
    boolean observe = operation == null || operation.hasMetadata();
    synchronized (watchers) {
      ImmutableList.Builder<Consumer<Operation>> observers = ImmutableList.builder();
      for (TimedWatchFuture watchFuture : operationWatchers) {
        TimedWatcher watcher = watchFuture.getWatcher();
        if (expiresAt != null) {
          watcher.reset(expiresAt);
        }
        if (shouldObserve.test(watcher)) {
          observers.add(watchFuture::observe);
        }
      }
      for (Consumer<Operation> observer : observers.build()) {
        executor.execute(() -> {
          if (observe) {
            observer.accept(operation);
          }
        });
      }
    }
  }

  @Override
  public void onMessage(String channel, String message) {
    if (message != null && message.equals("expire")) {
      terminateExpiredWatchers(channel, Instant.now());
    } else {
      Operation operation = message == null
          ? null : RedisShardBackplane.parseOperationJson(message);
      onOperation(channel, operation, nextExpiresAt());
    }
  }

  @Override
  public void onSubscribe(String channel, int subscribedChannels) {
  }

  @Override
  public void onUnsubscribe(String channel, int subscribedChannels) {
    List<TimedWatchFuture> operationWatchers;
    synchronized (watchers) {
      operationWatchers = watchers.removeAll(channel);
    }
    for (TimedWatchFuture watchFuture : operationWatchers) {
      watchFuture.complete();
    }
  }

  private String[] placeholderChannel() {
    String[] channels = new String[1];
    channels[0] = "placeholder-shard-subscription";
    return channels;
  }

  @Override
  public void proceed(Client client, String... channels) {
    if (channels.length == 0) {
      channels = placeholderChannel();
    }
    super.proceed(client, channels);
  }

  protected abstract Instant nextExpiresAt();
}
