// Copyright 2026 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.shard.backplane;

import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.redis.ScanCount;
import build.buildfarm.v1test.Digest;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import redis.clients.jedis.AbstractPipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.exceptions.JedisException;

/**
 * @class JedisCasWorkerMap
 * @brief A mapping from blob digest to the workers where the blobs reside.
 * @details This is used to identify the location of blobs within the shard. {blob digest ->
 *     set(worker1,worker2)}.
 */
public class JedisCasWorkerMap implements CasWorkerMap {
  private static final int PIPELINE_CHUNK_MAX = 10000;

  private final UnifiedJedis jedis;

  /**
   * @field name
   * @brief The unique name of the map.
   * @details The name is used in redis to store/access the data. If two maps had the same name,
   *     they would be instances of the same underlying redis map.
   */
  private final String name;

  /**
   * @field keyExpiration_s
   * @brief When keys will expire automatically.
   * @details This is currently the same for every key added or adjusted.
   * @note units: seconds
   */
  private final int keyExpiration_s;

  /**
   * @brief Constructor.
   * @details Construct storage object under the assumption that all calls will go to redis (no
   *     caching).
   * @param jedis Client used for interacting with redis.
   * @param name The global name of the map.
   * @param keyExpiration_s When to have keys expire automatically. (units: seconds (s))
   * @note Overloaded.
   */
  public JedisCasWorkerMap(UnifiedJedis jedis, String name, int keyExpiration_s) {
    this.jedis = jedis;
    this.name = name;
    this.keyExpiration_s = keyExpiration_s;
  }

  /**
   * @brief Adjust blob mappings based on worker changes.
   * @details Adjustments are made based on added and removed workers. Expirations are refreshed.
   * @param blobDigest The blob digest to adjust worker information from.
   * @param addWorkers Workers to add.
   * @param removeWorkers Workers to remove.
   */
  @Override
  public void adjust(Digest blobDigest, Set<String> addWorkers, Set<String> removeWorkers) {
    String key = redisCasKey(blobDigest);
    try (AbstractPipeline pipeline = jedis.pipelined()) {
      List<Response<?>> responses = new ArrayList<>();
      if (!addWorkers.isEmpty()) {
        responses.add(pipeline.sadd(key, addWorkers.toArray(String[]::new)));
      }
      if (!removeWorkers.isEmpty()) {
        responses.add(pipeline.srem(key, removeWorkers.toArray(String[]::new)));
      }
      responses.add(pipeline.expire(key, keyExpiration_s));
      syncAndCheck(pipeline, responses);
    }
  }

  /**
   * @brief Update the blob entry for the worker.
   * @details This may add a new key if the blob did not previously exist, or it will adjust the
   *     worker values based on the worker name. The expiration time is always refreshed.
   * @param client Client used for interacting with redis when not using cacheMap.
   * @param blobDigest The blob digest to adjust worker information from.
   * @param workerName The worker to add for looking up the blob.
   */
  @Override
  public void add(Digest blobDigest, String workerName) {
    String key = redisCasKey(blobDigest);
    try (AbstractPipeline pipeline = jedis.pipelined()) {
      Response<Long> addResponse = pipeline.sadd(key, workerName);
      Response<Long> expireResponse = pipeline.expire(key, keyExpiration_s);
      syncAndCheck(pipeline, List.of(addResponse, expireResponse));
    }
  }

  /**
   * @brief Update multiple blob entries for a worker.
   * @details This may add a new key if the blob did not previously exist, or it will adjust the
   *     worker values based on the worker name. The expiration time is always refreshed.
   * @param client Client used for interacting with redis when not using cacheMap.
   * @param blobDigests The blob digests to adjust worker information from.
   * @param workerName The worker to add for looking up the blobs.
   */
  @Override
  public void addAll(Iterable<Digest> blobDigests, String workerName) {
    for (List<Digest> chunk : Iterables.partition(blobDigests, PIPELINE_CHUNK_MAX)) {
      try (AbstractPipeline pipeline = jedis.pipelined()) {
        List<Response<?>> responses = new ArrayList<>();
        for (Digest blobDigest : chunk) {
          String key = redisCasKey(blobDigest);
          responses.add(pipeline.sadd(key, workerName));
          responses.add(pipeline.expire(key, keyExpiration_s));
        }
        syncAndCheck(pipeline, responses);
      }
    }
  }

  /**
   * @brief Remove worker value from blob key.
   * @details If the blob is already missing, or the worker doesn't exist, this will have no effect.
   * @param client Client used for interacting with redis when not using cacheMap.
   * @param blobDigest The blob digest to remove the worker from.
   * @param workerName The worker name to remove.
   */
  @Override
  public void remove(Digest blobDigest, String workerName) {
    String key = redisCasKey(blobDigest);
    jedis.srem(key, workerName);
  }

  /**
   * @brief Remove worker value from all blob keys.
   * @details If the blob is already missing, or the worker doesn't exist, this will be no effect on
   *     the key.
   * @param client Client used for interacting with redis when not using cacheMap.
   * @param blobDigests The blob digests to remove the worker from.
   * @param workerName The worker name to remove.
   */
  @Override
  public void removeAll(Iterable<Digest> blobDigests, String workerName) {
    for (List<Digest> chunk : Iterables.partition(blobDigests, PIPELINE_CHUNK_MAX)) {
      try (AbstractPipeline pipeline = jedis.pipelined()) {
        List<Response<?>> responses = new ArrayList<>();
        for (Digest blobDigest : chunk) {
          String key = redisCasKey(blobDigest);
          responses.add(pipeline.srem(key, workerName));
        }
        syncAndCheck(pipeline, responses);
      }
    }
  }

  /**
   * @brief Get a random worker for where the blob resides.
   * @details Picking a worker may done differently in the future.
   * @param client Client used for interacting with redis when not using cacheMap.
   * @param blobDigest The blob digest to lookup a worker for.
   * @return A worker for where the blob is.
   * @note Suggested return identifier: workerName.
   */
  @Override
  public String getAny(Digest blobDigest) {
    String key = redisCasKey(blobDigest);
    try (AbstractPipeline pipeline = jedis.pipelined()) {
      Response<Long> expiration = pipeline.expire(key, keyExpiration_s);
      Response<String> workers = pipeline.srandmember(key);
      syncAndCheck(pipeline, List.of(expiration, workers));
      return workers.get();
    }
  }

  /**
   * @brief Get all of the workers for where a blob resides.
   * @details Set is empty if the locaion of the blob is unknown.
   * @param client Client used for interacting with redis when not using cacheMap.
   * @param blobDigest The blob digest to lookup a worker for.
   * @return All the workers where the blob is expected to be.
   * @note Suggested return identifier: workerNames.
   */
  @Override
  public Set<String> get(Digest blobDigest) {
    String key = redisCasKey(blobDigest);
    try (AbstractPipeline pipeline = jedis.pipelined()) {
      Response<Long> expiration = pipeline.expire(key, keyExpiration_s);
      Response<Set<String>> workers = pipeline.smembers(key);
      syncAndCheck(pipeline, List.of(expiration, workers));
      return workers.get();
    }
  }

  @Override
  public long insertTime(Digest blobDigest) {
    String key = redisCasKey(blobDigest);
    return Instant.now().getEpochSecond() - keyExpiration_s + jedis.ttl(key);
  }

  /**
   * @brief Get all of the key values as a map from the digests given.
   * @details If there are no workers for the digest, the key is left out of the returned map.
   * @param client Client used for interacting with redis when not using cacheMap.
   * @param blobDigests The blob digests to get the key/values for.
   * @return The key/value map for digests to workers.
   * @note Suggested return identifier: casWorkerMap.
   */
  @Override
  public Map<Digest, Set<String>> getMap(Iterable<Digest> blobDigests) {
    ImmutableMap.Builder<Digest, Set<String>> blobDigestsWorkers = new ImmutableMap.Builder<>();
    for (List<Digest> chunk : Iterables.partition(blobDigests, PIPELINE_CHUNK_MAX)) {
      try (AbstractPipeline pipeline = jedis.pipelined()) {
        Map<Digest, Response<Set<String>>> responses = new HashMap<>();
        for (Digest blobDigest : chunk) {
          responses.put(blobDigest, pipeline.smembers(redisCasKey(blobDigest)));
        }
        pipeline.sync();
        for (Map.Entry<Digest, Response<Set<String>>> entry : responses.entrySet()) {
          Set<String> workers = entry.getValue().get();
          if (!workers.isEmpty()) {
            blobDigestsWorkers.put(entry.getKey(), workers);
          }
        }
      }
    }
    return blobDigestsWorkers.build();
  }

  /**
   * @brief Get the size of the map.
   * @details May be inefficient to due scanning into memory and deduplicating.
   * @param client Client used for interacting with redis when not using cacheMap.
   * @return The size of the map.
   * @note Suggested return identifier: size.
   */
  public int size() {
    return ScanCount.get(jedis, name + ":*", 1000);
  }

  @Override
  public void setExpire(Iterable<Digest> blobDigests) {
    for (List<Digest> chunk : Iterables.partition(blobDigests, PIPELINE_CHUNK_MAX)) {
      try (AbstractPipeline pipeline = jedis.pipelined()) {
        List<Response<?>> responses = new ArrayList<>();
        for (Digest blobDigest : chunk) {
          responses.add(pipeline.expire(redisCasKey(blobDigest), keyExpiration_s));
        }
        syncAndCheck(pipeline, responses);
      }
    }
  }

  /** Synchronizes the pipeline and aggregates response failures. */
  private static void syncAndCheck(
      AbstractPipeline pipeline, List<? extends Response<?>> responses) {
    pipeline.sync();
    List<RuntimeException> failures = new ArrayList<>();
    // Command errors are surfaced only when you call response.get()
    for (Response<?> response : responses) {
      try {
        response.get();
      } catch (RuntimeException e) {
        failures.add(e);
      }
    }
    if (!failures.isEmpty()) {
      JedisException exception =
          new JedisException("Pipeline failed with " + failures.size() + " response errors");
      failures.forEach(exception::addSuppressed);
      throw exception;
    }
  }

  /**
   * @brief Get the redis key name.
   * @details This is to be used for the direct redis implementation.
   * @param blobDigest The blob digest to be made part of the key.
   * @return The name of the key to use.
   * @note Suggested return identifier: keyName.
   */
  private String redisCasKey(Digest blobDigest) {
    return name + ":" + DigestUtil.toString(blobDigest);
  }
}
