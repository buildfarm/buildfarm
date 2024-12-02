// Copyright 2021 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.common.redis;

import static redis.clients.jedis.params.ScanParams.SCAN_POINTER_START;

import com.google.common.collect.Iterables;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;
import redis.clients.jedis.AbstractPipeline;
import redis.clients.jedis.Connection;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Response;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.resps.ScanResult;
import redis.clients.jedis.util.JedisClusterCRC16;

/**
 * @class RedisMap
 * @brief A redis map.
 * @details A redis map is an implementation of a map data structure which internally uses redis to
 *     store and distribute the data. Its important to know that the lifetime of the map persists
 *     before and after the map data structure is created (since it exists in redis). Therefore, two
 *     redis maps with the same name, would in fact be the same underlying redis map.
 */
public class RedisMap {
  /**
   * @field name
   * @brief The unique name of the map.
   * @details The name is used by the redis cluster client to access the map data. If two maps had
   *     the same name, they would be instances of the same underlying redis map.
   */
  private final String name;

  /**
   * @field expiration_s
   * @brief The expiration time to use on inserts when none is given.
   * @details The map can be initialized with a default expiration. In doing so, expirations can be
   *     omitted from calls to insert.
   */
  protected final int expiration_s;

  /**
   * @brief Constructor.
   * @details Construct a named redis map with an established redis cluster.
   * @param name The global name of the map.
   */
  public RedisMap(String name) {
    this(name, 24 * 60 * 60);
  }

  /**
   * @brief Constructor.
   * @details Construct a named redis map with an established redis cluster.
   * @param name The global name of the map.
   * @param timeout_s When to expire entries.
   */
  public RedisMap(String name, int timeout_s) {
    this.name = name;
    this.expiration_s = timeout_s;
  }

  /**
   * @brief Set key to hold the string value and set key to timeout after a given number of seconds.
   * @details If the key already exists, then the value is replaced.
   * @param jedis Jedis cluster client.
   * @param key The name of the key.
   * @param value The value for the key.
   * @param timeout_s Timeout to expire the entry. (units: seconds (s))
   * @note Overloaded.
   */
  public void insert(UnifiedJedis jedis, String key, String value, int timeout_s) {
    jedis.setex(createKeyName(key), timeout_s, value);
  }

  /**
   * @brief Set key to hold the string value and set key to timeout after a given number of seconds.
   * @details If the key already exists, then the value is replaced.
   * @param jedis Jedis cluster client.
   * @param key The name of the key.
   * @param value The value for the key.
   * @param timeout_s Timeout to expire the entry. (units: seconds (s))
   * @note Overloaded.
   */
  public void insert(UnifiedJedis jedis, String key, String value, long timeout_s) {
    // Jedis only provides int precision.  this is fine as the units are seconds.
    // We supply an interface for longs as a convenience to callers.
    jedis.setex(createKeyName(key), (int) timeout_s, value);
  }

  /**
   * @brief Set key to hold the string value and set key to timeout after a given number of seconds.
   * @details If the key already exists, then the value is replaced.
   * @param jedis Jedis cluster client.
   * @param key The name of the key.
   * @param value The value for the key.
   * @note Overloaded.
   */
  public void insert(UnifiedJedis jedis, String key, String value) {
    // Jedis only provides int precision.  this is fine as the units are seconds.
    // We supply an interface for longs as a convenience to callers.
    SetParams setParams = SetParams.setParams().ex(expiration_s);
    jedis.set(createKeyName(key), value, setParams);
  }

  public boolean putIfAbsent(UnifiedJedis jedis, String key, String value) {
    SetParams setParams = SetParams.setParams().nx().ex(expiration_s);
    return "OK".equals(jedis.set(createKeyName(key), value, setParams));
  }

  /**
   * @brief Remove a key from the map.
   * @details Deletes the key/value pair.
   * @param jedis Jedis cluster client.
   * @param key The name of the key.
   * @note Overloaded.
   */
  public void remove(UnifiedJedis jedis, String key) {
    jedis.del(createKeyName(key));
  }

  public void remove(AbstractPipeline pipeline, String key) {
    pipeline.del(createKeyName(key));
  }

  /**
   * @brief Remove multiple keys from the map.
   * @details Done via pipeline.
   * @param jedis Jedis cluster client.
   * @param keys The name of the keys.
   * @note Overloaded.
   */
  public void remove(UnifiedJedis jedis, Iterable<String> keys) {
    try (AbstractPipeline p = jedis.pipelined()) {
      for (String key : keys) {
        p.del(createKeyName(key));
      }
    }
  }

  /**
   * @brief Get the value of the key.
   * @details If the key does not exist, null is returned.
   * @param jedis Jedis cluster client.
   * @param key The name of the key.
   * @return The value of the key. null if key does not exist.
   * @note Overloaded.
   * @note Suggested return identifier: value.
   */
  public String get(UnifiedJedis jedis, String key) {
    return jedis.get(createKeyName(key));
  }

  /**
   * @brief Get the values of the keys.
   * @details If the key does not exist, null is returned.
   * @param jedis Jedis cluster client.
   * @param keys The name of the keys.
   * @return The values of the keys. null if key does not exist.
   * @note Overloaded.
   * @note Suggested return identifier: values.
   */
  public Iterable<Map.Entry<String, String>> get(UnifiedJedis jedis, Iterable<String> keys) {
    // Fetch items via pipeline
    try (AbstractPipeline p = jedis.pipelined()) {
      List<Map.Entry<String, Response<String>>> values = new ArrayList<>();
      StreamSupport.stream(keys.spliterator(), false)
          .forEach(
              key -> values.add(new AbstractMap.SimpleEntry<>(key, p.get(createKeyName(key)))));
      p.sync();

      List<Map.Entry<String, String>> resolved = new ArrayList<>();
      for (Map.Entry<String, Response<String>> val : values) {
        resolved.add(new AbstractMap.SimpleEntry<>(val.getKey(), val.getValue().get()));
      }
      return resolved;
    }
  }

  /**
   * @brief whether the key exists
   * @details True if key exists. False if key does not exist.
   * @param jedis Jedis cluster client.
   * @param key The name of the key.
   * @return Whether the key exists or not.
   * @note Suggested return identifier: exists.
   */
  public boolean exists(UnifiedJedis jedis, String key) {
    return jedis.exists(createKeyName(key));
  }

  /**
   * @brief Get the size of the map.
   * @details May be inefficient to due scanning into memory and deduplicating.
   * @param jedis Jedis cluster client.
   * @return The size of the map.
   * @note Suggested return identifier: size.
   */
  public int size(UnifiedJedis jedis) {
    return ScanCount.get(jedis, name + ":*", 1000);
  }

  /**
   * @brief Create the key name used in redis.
   * @details The key name is made more unique by leveraging the map's name.
   * @param key The name of the key.
   * @return The key name to use in redis.
   * @note Suggested return identifier: keyName.
   */
  protected String createKeyName(String key) {
    return name + ":" + key;
  }

  public ScanResult<String> scan(UnifiedJedis jedis, String mapCursor, int count) {
    return scan(jedis, mapCursor, count, /* match= */ "*");
  }

  public ScanResult<String> scan(UnifiedJedis jedis, String mapCursor, int count, String match) {
    if (jedis instanceof JedisCluster cluster) {
      return scanCluster(cluster, mapCursor, count, match);
    }
    return scanNode(jedis, mapCursor, count, match);
  }

  public ScanResult<String> scanCluster(
      JedisCluster cluster, String mapCursor, int count, String match) {
    int hashIndex = mapCursor.indexOf('{') + 1;
    String currentHash = null;
    if (hashIndex > 0) {
      int hashEnd = mapCursor.indexOf('}');
      currentHash = mapCursor.substring(hashIndex, hashEnd);
      mapCursor = mapCursor.substring(hashEnd + 1);
    }
    Iterator<String> hashes = RedisNodeHashes.getEvenlyDistributedHashes(cluster).iterator();

    // advance to our currentHash
    String hash = null;
    while (currentHash != null && !currentHash.equals(hash) && hashes.hasNext()) {
      hash = hashes.next();
    }
    // cursor specified hash not found
    if (currentHash != null && !currentHash.equals(hash)) {
      return new ScanResult<>(SCAN_POINTER_START, new ArrayList<>());
    }

    List<String> result = new ArrayList<>(count);
    while (result.size() < count) {
      if (currentHash == null || mapCursor.equals(SCAN_POINTER_START)) {
        if (!hashes.hasNext()) {
          break;
        }
        currentHash = hashes.next();
      }
      // acquire and assign resource to UnifiedJedis for release
      Connection connection = cluster.getConnectionFromSlot(JedisClusterCRC16.getSlot(currentHash));
      try (UnifiedJedis jedis = new UnifiedJedis(connection)) {
        ScanResult<String> scanResult = scanNode(jedis, mapCursor, count - result.size(), match);
        mapCursor = scanResult.getCursor();
        result.addAll(scanResult.getResult());
      }
    }

    if (mapCursor.equals(SCAN_POINTER_START)) {
      currentHash = null;
      if (hashes.hasNext()) {
        currentHash = hashes.next();
      }
    }
    String nextCursor = SCAN_POINTER_START;
    if (currentHash != null) {
      nextCursor = "{" + currentHash + "}" + mapCursor;
    }
    return new ScanResult(nextCursor, result);
  }

  private ScanResult<String> scanNode(
      UnifiedJedis jedis, String mapCursor, int count, String match) {
    // redis has much worse performance when scanning for small counts
    // break even is around 5k
    int scanCount = 5000;
    // maybe use type regular?
    ScanParams scanParams = new ScanParams().count(scanCount).match(createKeyName(match));
    int prefixLen = name.length() + 1;
    return new OffsetScanner<String>() {
      @Override
      protected ScanResult<String> scan(String cursor, int remaining) {
        return jedis.scan(cursor, scanParams);
      }

      @Override
      protected Iterable<String> transform(Iterable<String> result) {
        return Iterables.transform(result, operationName -> operationName.substring(prefixLen));
      }
    }.fill(mapCursor, count);
  }
}
