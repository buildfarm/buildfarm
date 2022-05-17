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
import java.util.concurrent.CompletableFuture;
import redis.clients.jedis.JedisCluster;

public class ShardActionCache {
  // L1 in-memory readthrough cache
  private final AsyncLoadingCache<ActionKey, ActionResult> readThroughCache;

  // L2 distributed cache
  private final RedisMap actionCache;
  private final int actionCacheExpire;

  public ShardActionCache(
      RedisClient client,
      String cachePrefix,
      int actionCacheExpire,
      int maxLocalCacheSize,
      ListeningExecutorService service) {
    this.actionCacheExpire = actionCacheExpire;
    readThroughCache = createReadThroughCache(client, service, maxLocalCacheSize);
    actionCache = new RedisMap(cachePrefix);
  }

  // Put key into all levels of cache.
  public void put(RedisClient client, ActionKey actionKey, ActionResult actionResult) {
    try {
      putL2(client, actionKey, actionResult);
    } catch (IOException e) {
      // this should be a non-grpc runtime exception
      throw Status.fromThrowable(e).asRuntimeException();
    }
    putL1(actionKey, actionResult);
  }

  // Get key by checking all levels of cache.
  public ListenableFuture<ActionResult> get(ActionKey actionKey) {
    return catching(
        toListenableFuture(readThroughCache.get(actionKey)),
        InvalidCacheLoadException.class,
        e -> null,
        directExecutor());
  }

  // Remove key from all levels of cache.
  public void remove(JedisCluster jedis, ActionKey actionKey) {
    removeL2(jedis, actionKey);
    removeL1(actionKey);
  }

  public int size(JedisCluster jedis) {
    return actionCache.size(jedis);
  }

  public void clear(JedisCluster jedis) {
    actionCache.clear(jedis);
    readThroughCache.synchronous().invalidateAll();
  }

  public void putL1(ActionKey actionKey, ActionResult actionResult) {
    readThroughCache.put(actionKey, CompletableFuture.completedFuture(actionResult));
  }

  public void putL2(RedisClient client, ActionKey actionKey, ActionResult actionResult)
      throws IOException {
    String json = JsonFormat.printer().print(actionResult);
    client.run(jedis -> actionCache.insert(jedis, asDigestStr(actionKey), json, actionCacheExpire));
  }

  public void removeL1(ActionKey actionKey) {
    readThroughCache.synchronous().invalidate(actionKey);
  }

  public void removeL2(JedisCluster jedis, ActionKey actionKey) {
    actionCache.remove(jedis, asDigestStr(actionKey));
  }

  private AsyncLoadingCache<ActionKey, ActionResult> createReadThroughCache(
      RedisClient client, ListeningExecutorService service, int maxLocalCacheSize) {
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

    return Caffeine.newBuilder().maximumSize(maxLocalCacheSize).buildAsync(loader);
  }

  private ActionResult parseActionResult(String json) {
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

    ActionResult actionResult = parseActionResult(json);
    if (actionResult == null) {
      client.run(jedis -> removeL2(jedis, actionKey));
    }
    return actionResult;
  }

  private String asDigestStr(ActionKey actionKey) {
    return DigestUtil.toString(actionKey.getDigest());
  }
}
