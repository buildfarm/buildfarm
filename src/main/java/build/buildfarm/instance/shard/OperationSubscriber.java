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

import com.google.common.collect.ImmutableList;
import com.google.longrunning.Operation;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

class OperationSubscriber extends JedisPubSub {

  private final Map<String, List<Predicate<Operation>>> watchers;
  private final Function<String, String> toOperationKey;

  OperationSubscriber(Map<String, List<Predicate<Operation>>> watchers, Function<String, String> toOperationKey) {
    this.watchers = watchers;
    this.toOperationKey = toOperationKey;
  }

  public List<String> watchedOperationNames() {
    Set<String> operationNameSet;
    synchronized (watchers) {
      operationNameSet = watchers.keySet();
    }
    int size = operationNameSet.size();
    if (size == 0) {
      return ImmutableList.of();
    }

    return new ArrayList<>(operationNameSet);
  }

  public void updateWatchedIfDone(Jedis jedis) {
    List<String> operationNames = watchedOperationNames();
    if (operationNames.size() == 0) {
      return;
    }

    System.out.println("OperationSubscriber::updateWatchedIfDone: Checking on " + operationNames.size() + " open watches");

    List<Map.Entry<String, Response<String>>> operations = new ArrayList(operationNames.size());
    Pipeline p = jedis.pipelined();
    for (String operationName : operationNames) {
      operations.add(new AbstractMap.SimpleEntry<>(
          operationName,
          p.get(toOperationKey.apply(operationName))));
    }
    p.sync();

    int iRemainingIncomplete = 20;
    for (Map.Entry<String, Response<String>> entry : operations) {
      String json = entry.getValue().get();
      Operation operation = json == null
          ? null : RedisShardBackplane.parseOperationJson(json);
      String operationName = entry.getKey();
      if (operation == null || operation.getDone()) {
        System.out.println("OperationSubscriber::updateWatchedIfDone: Operation " + operationName + " done due to " + (operation == null ? "null" : "completed"));
        onOperation(operationName, operation);
      } else if (iRemainingIncomplete > 0) {
        System.out.println("OperationSubscriber::updateWatchedIfDone: Operation " + operationName);
        iRemainingIncomplete--;
      }
    }
  }

  public void onOperation(String name, Operation operation) {
    synchronized (watchers) {
      List<Predicate<Operation>> operationWatchers =
          watchers.get(name);
      if (operationWatchers == null)
        return;

      boolean done = operation == null || operation.getDone();
      if (done) {
        watchers.remove(name);
      }

      long unfilteredWatcherCount = operationWatchers.size();
      ImmutableList.Builder<Predicate<Operation>> filteredWatchers = new ImmutableList.Builder<>();
      long filteredWatcherCount = 0;
      for (Predicate<Operation> watcher : operationWatchers) {
        if (watcher.test(operation) && !done) {
          filteredWatchers.add(watcher);
          filteredWatcherCount++;
        }
      }

      if (!done && filteredWatcherCount != unfilteredWatcherCount) {
        watchers.put(operation.getName(), new ArrayList<>(filteredWatchers.build()));
      }
    }
  }

  @Override
  public void onMessage(String channel, String message) {
    Operation operation = message == null
        ? null : RedisShardBackplane.parseOperationJson(message);
    String operationName = message == null
        ? channel : operation.getName();
    onOperation(operationName, operation);
  }
}
