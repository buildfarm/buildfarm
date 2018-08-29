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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.longrunning.Operation;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Predicate;
import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

class OperationSubscriber extends JedisPubSub {

  private final SetMultimap<String, Predicate<Operation>> watchers =
      Multimaps.<String, Predicate<Operation>>synchronizedSetMultimap(HashMultimap.<String, Predicate<Operation>>create());
  private final ListeningExecutorService executorService =
      MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(32));

  public List<String> watchedOperationChannels() {
    synchronized (watchers) {
      return ImmutableList.copyOf(watchers.keySet());
    }
  }

  // synchronizing on these because the client has been observed to
  // cause protocol desynchronization for multiple concurrent calls
  @Override
  public synchronized void subscribe(String... channels) {
    super.subscribe(channels);
  }

  @Override
  public synchronized void unsubscribe(String... channels) {
    super.unsubscribe(channels);
  }

  public void watch(String channel, Predicate<Operation> watcher) {
    // use prefix
    boolean hasSubscribed = watchers.containsKey(channel);
    watchers.put(channel, watcher);
    if (!hasSubscribed) {
      subscribe(channel);
    }
  }

  public void onOperation(String channel, Operation operation) {
    Set<Predicate<Operation>> operationWatchers = watchers.get(channel);
    boolean complete = operation == null || operation.getDone();
    ImmutableList.Builder<ListenableFuture<Boolean>> removedFutures =
        new ImmutableList.Builder<>();
    synchronized (watchers) {
      for (Predicate<Operation> watcher : operationWatchers) {
        ListenableFuture<Boolean> watcherFuture = executorService.submit(new Callable<Boolean>() {
          public Boolean call() {
            return watcher.test(operation) && !complete;
          }
        });
        ListenableFuture<Boolean> removeFuture = Futures.transform(watcherFuture, (stillWatching) -> {
          if (stillWatching) {
            return false;
          }

          synchronized (watchers) {
            operationWatchers.remove(watcher);
          }
          return true;
        }, executorService);
        removedFutures.add(removeFuture);
      }
    }

    Futures.addCallback(Futures.allAsList(removedFutures.build()), new FutureCallback<List<Boolean>>() {
      @Override
      public void onSuccess(List<Boolean> results) {
        if (Iterables.all(results, (result) -> result)) {
          unsubscribe(channel);
        }
      }

      @Override
      public void onFailure(Throwable t) {
        t.printStackTrace();
      }
    }, executorService);
  }

  @Override
  public void onMessage(String channel, String message) {
    Operation operation = message == null
        ? null : RedisShardBackplane.parseOperationJson(message);
    onOperation(channel, operation);
  }

  @Override
  public void onSubscribe(String channel, int subscribedChannels) {
  }

  @Override
  public void proceed(Client client, String... channels) {
    if (channels.length == 0) {
      channels = new String[1];
      channels[0] = "placeholder-shard-subscription";
    }
    super.proceed(client, channels);
  }
}
