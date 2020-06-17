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

import static redis.clients.jedis.JedisCluster.HASHSLOTS;

import build.buildfarm.common.redis.RedisSlotToHash;
import java.util.List;
import java.util.Set;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.util.JedisClusterCRC16;

///
/// @class   CasIndexer
/// @brief   Reindex CAS data in order to remove references on absent
///          workers.
/// @details Redis CAS entries contain reference information to the workers
///          who have access to the underlying addressable content. However
///          workers can disappear leaving some of these references stale. The
///          indexer is designed to compare existing workers with all CAS
///          entries and remove references to no longer accessible workers.
///
public class CasIndexer {

  ///
  /// @field   CAS_WILDCARD
  /// @brief   The prefix for finding CAS keys.
  /// @details Can be used in scan operations on CAS.
  ///
  private static final String CAS_WILDCARD = "ContentAddressableStorage:*";

  ///
  /// @field   DEFAULT_SCAN_COUNT
  /// @brief   A default scan count.
  /// @details Can be used in scan operations on CAS.
  ///
  private static final int DEFAULT_SCAN_COUNT = 10000;

  ///
  /// @field   WORKERS_KEY_NAME
  /// @brief   The worker key name in redis.
  /// @details Used to get all worker key names.
  ///
  private static final String WORKERS_KEY_NAME = "Workers";

  ///
  /// @field   TEMP_WORKER_SET
  /// @brief   The key name for a worker set.
  /// @details This set is temporary for performing same slot intersections.
  ///
  private static final String TEMP_WORKER_SET = "intersecting-workers";

  ///
  /// @field   CURSOR_SENTINAL
  /// @brief   Cursor sentimental used by jedis.
  /// @details Used to begin and end scan paging.
  ///
  private static final String CURSOR_SENTINAL = "0";

  ///
  /// @brief   Re-index the workers associated with CAS data.
  /// @details Removes references to worker nodes that are no longer running.
  /// @param   cluster An established jedis client to operate on a redis cluster.
  ///
  public static void reindexWorkers(JedisCluster cluster) {
    // setup
    storeActiveWorkersInAllSlots(cluster);

    // worker index deletion
    performIndexing(cluster);

    // cleanup
    deleteActiveWorkersFromAllSlots(cluster);
  }
  ///
  /// @brief   Perform the indexing operation to remove workers.
  /// @details Assumes the needed worker information is available in each slot.
  /// @param   cluster An established jedis client to operate on a redis cluster.
  ///
  private static void performIndexing(JedisCluster cluster) {
    // construct CAS query
    ScanParams params = new ScanParams();
    params.match(CAS_WILDCARD);
    params.count(DEFAULT_SCAN_COUNT);

    // run query and perform intersections
    String nextCursor = CURSOR_SENTINAL;
    do {

      // update workers on scanned keys
      ScanResult scanResult = cluster.scan(nextCursor, params);
      if (scanResult == null) {
        // no CAS keys available to reindex
        return;
      }
      List<String> keys = scanResult.getResult();
      deleteWorkersThroughIntersection(cluster, keys);

      nextCursor = scanResult.getCursor();
    } while (!nextCursor.equals(CURSOR_SENTINAL));
  }
  ///
  /// @brief   Reindex the workers through intersection.
  /// @details Uses all the CAS keys to update their references to workers.
  /// @param   cluster An established jedis client to operate on a redis cluster.
  /// @param   casKeys CAS keys to reindex.
  ///
  private static void deleteWorkersThroughIntersection(JedisCluster cluster, List<String> casKeys) {
    for (String casKey : casKeys) {
      int casSlotNumber = JedisClusterCRC16.getSlot(casKey);
      String workerKey = slotSpecificActiveWorkerSet(casSlotNumber);
      cluster.sinterstore(casKey, workerKey, casKey);
    }
  }
  ///
  /// @brief   Store the active workers in a set designated at each slot.
  /// @details This is done to ensure workers are available for intersection
  ///          operations at any slot.
  /// @param   cluster An established jedis client to operate on a redis cluster.
  ///
  private static void storeActiveWorkersInAllSlots(JedisCluster cluster) {
    Set<String> workers = cluster.hkeys(WORKERS_KEY_NAME);
    for (int i = 0; i < HASHSLOTS; ++i) {
      String key = slotSpecificActiveWorkerSet(i);
      cluster.del(key);
      cluster.sadd(key, workers.stream().toArray(String[]::new));
    }
  }
  ///
  /// @brief   Delete the active workers set from each slot.
  /// @details This is done assuming they are no longer needed.
  /// @param   cluster An established jedis client to operate on a redis cluster.
  ///
  private static void deleteActiveWorkersFromAllSlots(JedisCluster cluster) {
    for (int i = 0; i < HASHSLOTS; ++i) {
      String key = slotSpecificActiveWorkerSet(i);
      cluster.del(key);
    }
  }
  ///
  /// @brief   Get a set name specific to the slot number given.
  /// @details We need sets to exist at specific slots in order to perform
  ///          redis intersections (same slot operations).
  /// @param   slotNumber The slot number to derive the key name for.
  /// @return  The derived key with hashtag.
  /// @note    Suggested return identifier: key.
  ///
  private static String slotSpecificActiveWorkerSet(long slotNumber) {
    String hashtag = RedisSlotToHash.correlate(slotNumber);
    return "{" + hashtag + "}:" + TEMP_WORKER_SET;
  }
}
