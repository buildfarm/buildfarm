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
import build.buildfarm.common.redis.RedisClient;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.redisson.api.RSetMultimapCache;
import org.redisson.api.RedissonClient;

/**
 * @class RedissonCasWorkerMap
 * @brief A mapping from blob digest to the workers where the blobs reside.
 * @details This is used to identify the location of blobs within the shard. {blob digest ->
 *     set(worker1,worker2)}.
 */
public class RedissonCasWorkerMap implements CasWorkerMap {
  /**
   * @field keyExpiration_s
   * @brief When keys will expire automatically.
   * @details This is currently the same for every key added or adjusted.
   * @note units: seconds
   */
  private final int keyExpiration_s;

  /**
   * @field cacheMap
   * @brief A memory cached redis container to serve as the cas lookup.
   * @details This is only used if the object is configured to use a memory cache.
   */
  private final RSetMultimapCache<String, String> cacheMap;

  /**
   * @brief Constructor.
   * @details Construct storage object with options on how the data will be stored/accessed.
   * @param client The redisson client used to initialize the cache container.
   * @param name The global name of the map.
   * @param keyExpiration_s When to have keys expire automatically. (units: seconds (s))
   * @note Overloaded.
   */
  public RedissonCasWorkerMap(RedissonClient client, String name, int keyExpiration_s) {
    this.keyExpiration_s = keyExpiration_s;
    this.cacheMap = client.getSetMultimapCache(name);
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
      RedisClient client, Digest blobDigest, Set<String> addWorkers, Set<String> removeWorkers) {
    String key = cacheMapCasKey(blobDigest);
    cacheMap.putAll(key, addWorkers);
    for (String workerName : removeWorkers) {
      cacheMap.remove(key, workerName);
    }
    cacheMap.expireKey(key, keyExpiration_s, TimeUnit.SECONDS);
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
  public void add(RedisClient client, Digest blobDigest, String workerName) {
    String key = cacheMapCasKey(blobDigest);
    cacheMap.put(key, workerName);
    cacheMap.expireKey(key, keyExpiration_s, TimeUnit.SECONDS);
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
  public void addAll(RedisClient client, Iterable<Digest> blobDigests, String workerName) {
    for (Digest blobDigest : blobDigests) {
      String key = cacheMapCasKey(blobDigest);
      cacheMap.put(key, workerName);
      cacheMap.expireKey(key, keyExpiration_s, TimeUnit.SECONDS);
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
  public void remove(RedisClient client, Digest blobDigest, String workerName) {
    String key = cacheMapCasKey(blobDigest);
    cacheMap.remove(key, workerName);
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
  public void removeAll(RedisClient client, Iterable<Digest> blobDigests, String workerName) {
    for (Digest blobDigest : blobDigests) {
      String key = cacheMapCasKey(blobDigest);
      cacheMap.remove(key, workerName);
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
  public String getAny(RedisClient client, Digest blobDigest) {
    String key = cacheMapCasKey(blobDigest);
    Set<String> all = cacheMap.get(key).readAll();
    return getRandomElement(all);
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
  public Set<String> get(RedisClient client, Digest blobDigest) {
    String key = cacheMapCasKey(blobDigest);
    return cacheMap.get(key).readAll();
  }

  @Override
  public long insertTime(RedisClient client, Digest blobDigest) {
    String key = cacheMapCasKey(blobDigest);
    return Instant.now().getEpochSecond() - keyExpiration_s + cacheMap.get(key).remainTimeToLive();
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
  public Map<Digest, Set<String>> getMap(RedisClient client, Iterable<Digest> blobDigests) {
    ImmutableMap.Builder<Digest, Set<String>> blobDigestsWorkers = new ImmutableMap.Builder<>();
    for (Digest blobDigest : blobDigests) {
      String key = cacheMapCasKey(blobDigest);
      Set<String> workers = cacheMap.get(key).readAll();

      if (workers.isEmpty()) {
        continue;
      }
      blobDigestsWorkers.put(blobDigest, workers);
    }
    return blobDigestsWorkers.build();
  }

  /**
   * @brief Get the size of the map.
   * @param client Client used for interacting with redis when not using cacheMap.
   * @return The size of the map.
   * @note Suggested return identifier: size.
   */
  public int size(RedisClient client) {
    return cacheMap.size();
  }

  /**
   * @brief Get a random element from the set.
   * @details Assumes the set is not empty.
   * @param set The set to get a random element from.
   * @return A random element from the set.
   * @note Suggested return identifier: randomElement.
   */
  private <T> T getRandomElement(Set<T> set) {
    return set.stream().skip(new Random().nextInt(set.size())).findFirst().orElse(null);
  }

  /**
   * @brief Get the cacheMap key name.
   * @details This is to be used for the cache map implementation.
   * @param blobDigest The blob digest to be made part of the key.
   * @return The name of the key to use.
   * @note Suggested return identifier: keyName.
   */
  private String cacheMapCasKey(Digest blobDigest) {
    return DigestUtil.toString(blobDigest);
  }
}
