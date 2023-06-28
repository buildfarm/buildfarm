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

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.ScanCount;
import build.buildfarm.common.redis.RedisClient;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import redis.clients.jedis.JedisClusterPipeline;

/**
 * @class JedisCasWorkerMap
 * @brief A mapping from blob digest to the workers where the blobs reside.
 * @details This is used to identify the location of blobs within the shard. {blob digest ->
 *     set(worker1,worker2)}.
 */
public class JedisCasWorkerMap implements CasWorkerMap {
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
   * @param name The global name of the map.
   * @param keyExpiration_s When to have keys expire automatically. (units: seconds (s))
   * @note Overloaded.
   */
  public JedisCasWorkerMap(String name, int keyExpiration_s) {
    this.name = name;
    this.keyExpiration_s = keyExpiration_s;
  }

  /**
   * @brief Adjust blob mappings based on worker changes.
   * @details Adjustments are made based on added and removed workers. Expirations are refreshed.
   * @param client Client used for interacting with redis when not using cacheMap.
   * @param blobDigest The blob digest to adjust worker information from.
   * @param addWorkers Workers to add.
   * @param removeWorkers Workers to remove.
   */
  @Override
  public void adjust(
      RedisClient client, Digest blobDigest, Set<String> addWorkers, Set<String> removeWorkers)
      throws IOException {
    String key = redisCasKey(blobDigest);
    client.run(
        jedis -> {
          for (String workerName : addWorkers) {
            jedis.sadd(key, workerName);
          }
          for (String workerName : removeWorkers) {
            jedis.srem(key, workerName);
          }
          jedis.expire(key, keyExpiration_s);
        });
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
  public void add(RedisClient client, Digest blobDigest, String workerName) throws IOException {
    String key = redisCasKey(blobDigest);
    client.run(
        jedis -> {
          jedis.sadd(key, workerName);
          jedis.expire(key, keyExpiration_s);
        });
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
  public void addAll(RedisClient client, Iterable<Digest> blobDigests, String workerName)
      throws IOException {
    client.run(
        jedis -> {
          JedisClusterPipeline p = jedis.pipelined();
          for (Digest blobDigest : blobDigests) {
            String key = redisCasKey(blobDigest);
            p.sadd(key, workerName);
            p.expire(key, keyExpiration_s);
          }
          p.sync();
        });
  }

  /**
   * @brief Remove worker value from blob key.
   * @details If the blob is already missing, or the worker doesn't exist, this will have no effect.
   * @param client Client used for interacting with redis when not using cacheMap.
   * @param blobDigest The blob digest to remove the worker from.
   * @param workerName The worker name to remove.
   */
  @Override
  public void remove(RedisClient client, Digest blobDigest, String workerName) throws IOException {
    String key = redisCasKey(blobDigest);
    client.run(jedis -> jedis.srem(key, workerName));
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
  public void removeAll(RedisClient client, Iterable<Digest> blobDigests, String workerName)
      throws IOException {
    client.run(
        jedis -> {
          JedisClusterPipeline p = jedis.pipelined();
          for (Digest blobDigest : blobDigests) {
            String key = redisCasKey(blobDigest);
            p.srem(key, workerName);
          }
          p.sync();
        });
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
  public String getAny(RedisClient client, Digest blobDigest) throws IOException {
    String key = redisCasKey(blobDigest);
    return client.call(jedis -> jedis.srandmember(key));
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
  public Set<String> get(RedisClient client, Digest blobDigest) throws IOException {
    String key = redisCasKey(blobDigest);
    return client.call(jedis -> jedis.smembers(key));
  }

  @Override
  public long insertTime(RedisClient client, Digest blobDigest) throws IOException {
    String key = redisCasKey(blobDigest);
    return Instant.now().getEpochSecond() - keyExpiration_s + client.call(jedis -> jedis.ttl(key));
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
  public Map<Digest, Set<String>> getMap(RedisClient client, Iterable<Digest> blobDigests)
      throws IOException {
    ImmutableMap.Builder<Digest, Set<String>> blobDigestsWorkers = new ImmutableMap.Builder<>();
    client.run(
        jedis -> {
          for (Digest blobDigest : blobDigests) {
            String key = redisCasKey(blobDigest);
            Set<String> workers = jedis.smembers(key);

            if (workers.isEmpty()) {
              continue;
            }
            blobDigestsWorkers.put(blobDigest, workers);
          }
        });
    return blobDigestsWorkers.build();
  }

  /**
   * @brief Get the size of the map.
   * @details May be inefficient to due scanning into memory and deduplicating.
   * @param client Client used for interacting with redis when not using cacheMap.
   * @return The size of the map.
   * @note Suggested return identifier: size.
   */
  public int size(RedisClient client) throws IOException {
    return client.call(jedis -> ScanCount.get(jedis, name + ":*", 1000));
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
