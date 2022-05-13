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

package build.buildfarm.instance.shard;

import static com.google.common.util.concurrent.Futures.catching;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static net.javacrumbs.futureconverter.java8guava.FutureConverter.toCompletableFuture;
import static net.javacrumbs.futureconverter.java8guava.FutureConverter.toListenableFuture;

import build.bazel.remote.execution.v2.ActionResult;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.redis.RedisClient;
import build.buildfarm.common.redis.RedisMap;
import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.cache.CacheLoader.InvalidCacheLoadException;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Status;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import redis.clients.jedis.JedisCluster;

public class ShardActionCache {
  private final RedisMap actionCache;
  private final AsyncLoadingCache<ActionKey, ActionResult> actionResultCache;
  private final int actionCacheExpire;

  public ShardActionCache(
      RedisClient client,
      String cachePrefix,
      int actionCacheExpire,
      int maxLocalCacheSize,
      ListeningExecutorService service) {
    AsyncCacheLoader<ActionKey, ActionResult> loader =
        (actionKey, executor) ->
            toCompletableFuture(
                catching(
                    service.submit(() -> getActionResult(client, actionKey)),
                    IOException.class,
                    e -> {
                      throw Status.fromThrowable(e).asRuntimeException();
                    },
                    executor));

    actionResultCache = Caffeine.newBuilder().maximumSize(maxLocalCacheSize).buildAsync(loader);

    this.actionCacheExpire = actionCacheExpire;
    actionCache = new RedisMap(cachePrefix);
  }

  // L1->L2
  public void put(RedisClient client, ActionKey actionKey, ActionResult actionResult) {
    try {
      putL2(client, actionKey, actionResult);
    } catch (IOException e) {
      // this should be a non-grpc runtime exception
      throw Status.fromThrowable(e).asRuntimeException();
    }
    readThrough(actionKey, actionResult);
  }

  // L1<-L2
  public ListenableFuture<ActionResult> get(ActionKey actionKey) {
    return catching(
        toListenableFuture(actionResultCache.get(actionKey)),
        InvalidCacheLoadException.class,
        e -> null,
        directExecutor());
  }

  public void remove(JedisCluster jedis, ActionKey actionKey) {
    actionCache.remove(jedis, asDigestStr(actionKey));
  }

  public void remove(RedisClient client, Iterable<ActionKey> actionKeys) throws IOException {
    // convert action keys to strings
    List<String> keyNames = new ArrayList<>();
    actionKeys.forEach(key -> keyNames.add(asDigestStr(key)));

    client.run(jedis -> actionCache.remove(jedis, keyNames));
  }

  // Invalidate L1 cache only
  public void invalidate(ActionKey actionKey) {
    actionResultCache.synchronous().invalidate(actionKey);
  }

  // Add to L1 cache only
  public void readThrough(ActionKey actionKey, ActionResult actionResult) {
    actionResultCache.put(actionKey, CompletableFuture.completedFuture(actionResult));
  }

  public int size(JedisCluster jedis) {
    return actionCache.size(jedis);
  }

  private static ActionResult parseActionResult(String json) {
    try {
      ActionResult.Builder builder = ActionResult.newBuilder();
      JsonFormat.parser().merge(json, builder);
      return builder.build();
    } catch (InvalidProtocolBufferException e) {
      return null;
    }
  }

  private ActionResult getActionResult(RedisClient client, ActionKey actionKey) throws IOException {
    String json = client.call(jedis -> actionCache.get(jedis, asDigestStr(actionKey)));
    if (json == null) {
      return null;
    }

    ActionResult actionResult = parseActionResult(json);
    if (actionResult == null) {
      client.run(jedis -> removeActionResult(jedis, actionKey));
    }
    return actionResult;
  }

  private void putL2(RedisClient client, ActionKey actionKey, ActionResult actionResult)
      throws IOException {
    String json = JsonFormat.printer().print(actionResult);
    client.run(jedis -> actionCache.insert(jedis, asDigestStr(actionKey), json, actionCacheExpire));
  }

  private void removeActionResult(JedisCluster jedis, ActionKey actionKey) {
    actionCache.remove(jedis, asDigestStr(actionKey));
  }

  private String asDigestStr(ActionKey actionKey) {
    return DigestUtil.toString(actionKey.getDigest());
  }
}
