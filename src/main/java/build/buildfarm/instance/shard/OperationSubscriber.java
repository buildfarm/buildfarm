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

import static java.util.logging.Level.SEVERE;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.longrunning.Operation;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.function.Predicate;
import java.util.logging.Logger;
import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

abstract class OperationSubscriber extends JedisPubSub {
  private static final Logger logger = Logger.getLogger(OperationSubscriber.class.getName());

  private final ListMultimap<String, TimedWatcher<Operation>> watchers;
  private final ListeningExecutorService executorService;

  OperationSubscriber(
      ListMultimap<String, TimedWatcher<Operation>> watchers,
      ExecutorService executorService) {
    this.watchers = watchers;
    this.executorService = MoreExecutors.listeningDecorator(executorService);
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
        for (TimedWatcher<Operation> watcher : watchers.get(channel)) {
          if (watcher.isExpiredAt(now)) {
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
  public synchronized void subscribe(String... channels) {
    super.subscribe(channels);
  }

  @Override
  public synchronized void unsubscribe() {
    super.unsubscribe();
  }

  @Override
  public synchronized void unsubscribe(String... channels) {
    super.unsubscribe(channels);
  }

  public void watch(String channel, TimedWatcher<Operation> watcher) {
    boolean hasSubscribed;
    synchronized (watchers) {
      // use prefix
      hasSubscribed = watchers.containsKey(channel);
      watchers.put(channel, watcher);
      if (!hasSubscribed) {
        subscribe(channel);
      }
    }
  }

  public void resetWatchers(String channel, Instant expiresAt) {
    List<TimedWatcher<Operation>> operationWatchers = watchers.get(channel);
    synchronized (watchers) {
      for (TimedWatcher<Operation> watcher : operationWatchers) {
        watcher.reset(expiresAt);
      }
    }
  }

  public void terminateExpiredWatchers(String channel, Instant now) {
    onOperation(channel, null, (watcher) -> {
      boolean expired = watcher.isExpiredAt(now);
      if (expired) {
        System.out.println("Terminating expired watcher of " + channel + " because: " + now + " >= " + watcher.getExpiresAt());
      }
      return expired;
    }, now);
  }

  public void onOperation(String channel, Operation operation, Instant expiresAt) {
    onOperation(channel, operation, (watcher) -> true, expiresAt);
  }

  private static class StillWatchingWatcher {
    public final boolean stillWatching;
    public final TimedWatcher<Operation> watcher;

    StillWatchingWatcher(boolean stillWatching, TimedWatcher<Operation> watcher) {
      this.stillWatching = stillWatching;
      this.watcher = watcher;
    }
  }

  private void onOperation(
      String channel,
      Operation operation,
      Predicate<TimedWatcher<Operation>> shouldObserve,
      Instant expiresAt) {
    List<TimedWatcher<Operation>> operationWatchers = watchers.get(channel);
    boolean complete = operation == null || operation.getDone();
    boolean observe = operation == null || operation.hasMetadata();
    ImmutableList.Builder<ListenableFuture<StillWatchingWatcher>> stillWatchingWatcherFutures =
        new ImmutableList.Builder<>();
    synchronized (watchers) {
      if (operationWatchers.isEmpty()) {
        return;
      }

      for (TimedWatcher<Operation> watcher : operationWatchers) {
        if (shouldObserve.test(watcher)) {
          ListenableFuture<StillWatchingWatcher> watcherFuture =
              Futures.transform(
                  executorService.submit(new Callable<Boolean>() {
                    public Boolean call() {
                      boolean stillWatching = (!observe || watcher.observe(operation)) && !complete;
                      watcher.reset(expiresAt);
                      return stillWatching;
                    }
                  }),
                  (stillWatching) -> new StillWatchingWatcher(stillWatching, watcher),
                  executorService);
          stillWatchingWatcherFutures.add(watcherFuture);
        }
      }
    }

    Futures.addCallback(
        Futures.allAsList(stillWatchingWatcherFutures.build()),
        new FutureCallback<List<StillWatchingWatcher>>() {
          @Override
          public void onSuccess(List<StillWatchingWatcher> stillWatchingWatchers) {
            List<Watcher> unwatched = ImmutableList.copyOf(
                Iterables.transform(
                    Iterables.filter(stillWatchingWatchers, (s) -> !s.stillWatching),
                    (s) -> s.watcher));
            synchronized (watchers) {
              // a little unoptimized for a list, perhaps we should save the index...
              operationWatchers.removeAll(unwatched);
              if (operationWatchers.isEmpty()) {
                unsubscribe(channel);
              }
            }
          }

          @Override
          public void onFailure(Throwable t) {
            logger.log(SEVERE, "error while filtering watchers", t);
          }
        },
        executorService);
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
