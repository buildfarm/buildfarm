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
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.redisson.api.RSetMultimapCache;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisClusterPipeline;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

///
/// @class   JedisCasWorkerMap
/// @brief   A mapping from blob digest to the workers where the blobs
///          reside.
/// @details This is used to identify the location of blobs within the shard.
///          {blob digest -> set(worker1,worker2)}.
///
public class JedisCasWorkerMap implements AbstractCasWorkerMap {

  ///
  /// @field   name
  /// @brief   The unique name of the map.
  /// @details The name is used in redis to store/access the data. If two maps
  ///          had the same name, they would be instances of the same underlying
  ///          redis map.
  ///
  private final String name;

  ///
  /// @field   keyExpiration_s
  /// @brief   When keys will expire automatically.
  /// @details This is currently the same for every key added or adjusted.
  /// @note    units: seconds
  ///
  private final int keyExpiration_s;

  ///
  /// @field   cacheMap
  /// @brief   A memory cached redis container to serve as the cas lookup.
  /// @details This is only used if the object is configured to use a memory
  ///          cache.
  ///
  private RSetMultimapCache<String, String> cacheMap;

  ///
  /// @brief   Constructor.
  /// @details Construct storage object under the assumption that all calls
  ///          will go to redis (no caching).
  /// @param   name            The global name of the map.
  /// @param   keyExpiration_s When to have keys expire automatically. (units: seconds (s))
  /// @note    Overloaded.
  ///
  public JedisCasWorkerMap(String name, int keyExpiration_s) {
    this.name = name;
    this.keyExpiration_s = keyExpiration_s;
  }
  ///
  /// @brief   Adjust blob mappings based on worker changes.
  /// @details Adjustments are made based on added and removed workers.
  ///          Expirations are refreshed.
  /// @param   client        Client used for interacting with redis when not using cacheMap.
  /// @param   blobDigest    The blob digest to adjust worker information from.
  /// @param   addWorkers    Workers to add.
  /// @param   removeWorkers Workers to remove.
  ///
  @Override
  public void adjust(
      RedisClient client, Digest blobDigest, Set<String> addWorkers, Set<String> removeWorkers)
      throws IOException {
    adjustJedis(client, blobDigest, addWorkers, removeWorkers);
  }
  ///
  /// @brief   Update the blob entry for the worker.
  /// @details This may add a new key if the blob did not previously exist, or
  ///          it will adjust the worker values based on the worker name. The
  ///          expiration time is always refreshed.
  /// @param   client     Client used for interacting with redis when not using cacheMap.
  /// @param   blobDigest The blob digest to adjust worker information from.
  /// @param   workerName The worker to add for looking up the blob.
  ///
  @Override
  public void add(RedisClient client, Digest blobDigest, String workerName) throws IOException {
    addJedis(client, blobDigest, workerName);
  }
  ///
  /// @brief   Update multiple blob entries for a worker.
  /// @details This may add a new key if the blob did not previously exist, or
  ///          it will adjust the worker values based on the worker name. The
  ///          expiration time is always refreshed.
  /// @param   client      Client used for interacting with redis when not using cacheMap.
  /// @param   blobDigests The blob digests to adjust worker information from.
  /// @param   workerName  The worker to add for looking up the blobs.
  ///
  @Override
  public void addAll(RedisClient client, Iterable<Digest> blobDigests, String workerName)
      throws IOException {
    addAllJedis(client, blobDigests, workerName);
  }
  ///
  /// @brief   Remove worker value from blob key.
  /// @details If the blob is already missing, or the worker doesn't exist,
  ///          this will have no effect.
  /// @param   client     Client used for interacting with redis when not using cacheMap.
  /// @param   blobDigest The blob digest to remove the worker from.
  /// @param   workerName The worker name to remove.
  ///
  @Override
  public void remove(RedisClient client, Digest blobDigest, String workerName) throws IOException {
    removeJedis(client, blobDigest, workerName);
  }
  ///
  /// @brief   Remove worker value from all blob keys.
  /// @details If the blob is already missing, or the worker doesn't exist,
  ///          this will be no effect on the key.
  /// @param   client      Client used for interacting with redis when not using cacheMap.
  /// @param   blobDigests The blob digests to remove the worker from.
  /// @param   workerName  The worker name to remove.
  ///
  @Override
  public void removeAll(RedisClient client, Iterable<Digest> blobDigests, String workerName)
      throws IOException {
    removeAllJedis(client, blobDigests, workerName);
  }
  ///
  /// @brief   Get a random worker for where the blob resides.
  /// @details Picking a worker may done differently in the future.
  /// @param   client     Client used for interacting with redis when not using cacheMap.
  /// @param   blobDigest The blob digest to lookup a worker for.
  /// @return  A worker for where the blob is.
  /// @note    Suggested return identifier: workerName.
  ///
  @Override
  public String getAny(RedisClient client, Digest blobDigest) throws IOException {
    return getAnyJedis(client, blobDigest);
  }
  ///
  /// @brief   Get all of the workers for where a blob resides.
  /// @details Set is empty if the locaion of the blob is unknown.
  /// @param   client     Client used for interacting with redis when not using cacheMap.
  /// @param   blobDigest The blob digest to lookup a worker for.
  /// @return  All the workers where the blob is expected to be.
  /// @note    Suggested return identifier: workerNames.
  ///
  @Override
  public Set<String> get(RedisClient client, Digest blobDigest) throws IOException {
    return getJedis(client, blobDigest);
  }
  ///
  /// @brief   Get all of the key values as a map from the digests given.
  /// @details If there are no workers for the digest, the key is left out of
  ///          the returned map.
  /// @param   client      Client used for interacting with redis when not using cacheMap.
  /// @param   blobDigests The blob digests to get the key/values for.
  /// @return  The key/value map for digests to workers.
  /// @note    Suggested return identifier: casWorkerMap.
  ///
  @Override
  public Map<Digest, Set<String>> getMap(RedisClient client, Iterable<Digest> blobDigests)
      throws IOException {
    return getMapJedis(client, blobDigests);
  }

  ///
  /// @brief   Get the size of the map.
  /// @details Returns the number of key-value pairs in this multimap.
  /// @param   client      Client used for interacting with redis when not using cacheMap.
  /// @return  The size of the map.
  /// @note    Suggested return identifier: mapSize.
  ///
  @Override
  public int size(RedisClient client) throws IOException {
    return client.call(jedis -> scanSize(jedis, name + ":*"));
  }

  ///
  /// @brief   Calculate the size of the container through scanning
  /// @details We use a wildcard to scan over all elements
  /// @param   cluster  An established redis cluster.
  /// @return  The total number of keys scanned correlating to the size
  /// @note    Suggested return identifier: size
  ///
  private int scanSize(JedisCluster cluster, String query) {

    Set<String> keys = Sets.newHashSet();

    // JedisCluster only supports SCAN commands with MATCH patterns containing hash-tags.
    // This prevents us from using the cluster's SCAN to traverse all of the CAS.
    // That's why we choose to scan each of the jedisNode's individually.
    cluster.getClusterNodes().values().stream()
        .forEach(
            pool -> {
              try (Jedis node = pool.getResource()) {
                addKeys(cluster, node, query, keys);
              }
            });

    return keys.size();
  }

  ///
  /// @brief   Scan all CAS entires to accumulate a key count.
  /// @details keys are accumulated onto.
  /// @param   cluster An established redis cluster.
  /// @param   node    A node of the cluster.
  /// @param   keys    keys to accumulate while scanning.
  ///
  private void addKeys(JedisCluster cluster, Jedis node, String query, Set<String> keys) {
    // iterate over all CAS entries via scanning
    String cursor = "0";
    do {
      keys.addAll(scanCas(node, query, cursor));

    } while (!cursor.equals("0"));
  }

  ///
  /// @brief   Scan the cas to obtain CAS keys.
  /// @details Scanning is done incrementally via a cursor.
  /// @param   node     A node of the cluster.
  /// @param   cursor   Scan cursor.
  /// @return  Resulting CAS keys from scanning.
  /// @note    Suggested return identifier: casKeys.
  ///
  private List<String> scanCas(Jedis node, String query, String cursor) {
    // construct CAS query
    ScanParams params = new ScanParams();
    params.match(query);
    params.count(1000);

    // perform scan iteration
    ScanResult scanResult = node.scan(cursor, params);
    if (scanResult != null) {
      cursor = scanResult.getCursor();
      return scanResult.getResult();
    }
    return new ArrayList<>();
  }

  ///
  /// @brief   Adjust blob mappings based on worker changes.
  /// @details Adjustments are made based on added and removed workers.
  ///          Expirations are refreshed.
  /// @param   client        Client used for interacting with redis when not using cacheMap.
  /// @param   blobDigest    The blob digest to adjust worker information from.
  /// @param   addWorkers    Workers to add.
  /// @param   removeWorkers Workers to remove.
  ///
  private void adjustJedis(
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

  ///
  /// @brief   Update the blob entry for the worker.
  /// @details This may add a new key if the blob did not previously exist, or
  ///          it will adjust the worker values based on the worker name. The
  ///          expiration time is always refreshed.
  /// @param   client     Client used for interacting with redis when not using cacheMap.
  /// @param   blobDigest The blob digest to adjust worker information from.
  /// @param   workerName The worker to add for looking up the blob.
  ///
  private void addJedis(RedisClient client, Digest blobDigest, String workerName)
      throws IOException {
    String key = redisCasKey(blobDigest);
    client.run(
        jedis -> {
          jedis.sadd(key, workerName);
          jedis.expire(key, keyExpiration_s);
        });
  }

  ///
  /// @brief   Update multiple blob entries for a worker.
  /// @details This may add a new key if the blob did not previously exist, or
  ///          it will adjust the worker values based on the worker name. The
  ///          expiration time is always refreshed.
  /// @param   client      Client used for interacting with redis when not using cacheMap.
  /// @param   blobDigests The blob digests to adjust worker information from.
  /// @param   workerName  The worker to add for looking up the blobs.
  ///
  private void addAllJedis(RedisClient client, Iterable<Digest> blobDigests, String workerName)
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

  ///
  /// @brief   Remove worker value from blob key.
  /// @details If the blob is already missing, or the worker doesn't exist,
  ///          this will have no effect.
  /// @param   client     Client used for interacting with redis when not using cacheMap.
  /// @param   blobDigest The blob digest to remove the worker from.
  /// @param   workerName The worker name to remove.
  ///
  private void removeJedis(RedisClient client, Digest blobDigest, String workerName)
      throws IOException {
    String key = redisCasKey(blobDigest);
    client.run(jedis -> jedis.srem(key, workerName));
  }

  ///
  /// @brief   Remove worker value from all blob keys.
  /// @details If the blob is already missing, or the worker doesn't exist,
  ///          this will be no effect on the key.
  /// @param   client      Client used for interacting with redis when not using cacheMap.
  /// @param   blobDigests The blob digests to remove the worker from.
  /// @param   workerName  The worker name to remove.
  ///
  private void removeAllJedis(RedisClient client, Iterable<Digest> blobDigests, String workerName)
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

  ///
  /// @brief   Get a random worker for where the blob resides.
  /// @details Picking a worker may done differently in the future.
  /// @param   client     Client used for interacting with redis when not using cacheMap.
  /// @param   blobDigest The blob digest to lookup a worker for.
  /// @return  A worker for where the blob is.
  /// @note    Suggested return identifier: workerName.
  ///
  private String getAnyJedis(RedisClient client, Digest blobDigest) throws IOException {
    String key = redisCasKey(blobDigest);
    return client.call(jedis -> jedis.srandmember(key));
  }
  ///
  /// @brief   Get all of the workers for where a blob resides.
  /// @details Set is empty if the locaion of the blob is unknown.
  /// @param   client     Client used for interacting with redis when not using cacheMap.
  /// @param   blobDigest The blob digest to lookup a worker for.
  /// @return  All the workers where the blob is expected to be.
  /// @note    Suggested return identifier: workerNames.
  ///
  private Set<String> getJedis(RedisClient client, Digest blobDigest) throws IOException {
    String key = redisCasKey(blobDigest);
    return client.call(jedis -> jedis.smembers(key));
  }
  ///
  /// @brief   Get all of the key values as a map from the digests given.
  /// @details If there are no workers for the digest, the key is left out of
  ///          the returned map.
  /// @param   client      Client used for interacting with redis when not using cacheMap.
  /// @param   blobDigests The blob digests to get the key/values for.
  /// @return  The key/value map for digests to workers.
  /// @note    Suggested return identifier: casWorkerMap.
  ///
  private Map<Digest, Set<String>> getMapJedis(RedisClient client, Iterable<Digest> blobDigests)
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
  ///
  /// @brief   Get the redis key name.
  /// @details This is to be used for the direct redis implementation.
  /// @param   blobDigest The blob digest to be made part of the key.
  /// @return  The name of the key to use.
  /// @note    Suggested return identifier: keyName.
  ///
  private String redisCasKey(Digest blobDigest) {
    return name + ":" + DigestUtil.toString(blobDigest);
  }
}
