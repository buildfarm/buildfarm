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
import java.util.Set;
import redis.clients.jedis.JedisCluster;

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
  /// @brief   Re-index the workers associated with CAS data.
  /// @details Removes references to worker nodes that are no longer running.
  /// @param   cluster An established jedis client to operate on a redis cluster.
  ///
  public static void reindexWorkers(JedisCluster cluster) {
    storeActiveWorkersInAllSlots(cluster);
    deleteActiveWorkersFromAllSlots(cluster);
  }
  ///
  /// @brief   Store the active workers in a set designated at each slot.
  /// @details This is done to ensure workers are available for intersection
  ///          operations at any slot.
  /// @param   cluster An established jedis client to operate on a redis cluster.
  ///
  private static void storeActiveWorkersInAllSlots(JedisCluster cluster) {
    Set<String> workers = cluster.hkeys("Workers");
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
    return "{" + hashtag + "}:intersecting-workers";
  }
}
