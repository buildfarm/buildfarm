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

package build.buildfarm.common.redis;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.exceptions.JedisNoReachableClusterNodeException;

/**
 * @class RedisNodeHashes
 * @brief A list of redis hashtags that each map to different nodes in the cluster.
 * @details When looking to evenly distribute keys across nodes, specific hashtags need obtained in
 *     which each hashtag hashes to a particular slot owned by a particular worker. This class is
 *     used to obtain the hashtags needed to hit every node in the cluster.
 */
public class RedisNodeHashes {
  /**
   * @brief Get a list of evenly distributing hashtags for the provided redis cluster.
   * @details Each hashtag will map to a slot on a different node.
   * @param jedis An established jedis client.
   * @return Hashtags that will each has to a slot on a different node.
   * @note Suggested return identifier: hashtags.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static List<String> getEvenlyDistributedHashes(JedisCluster jedis) {
    try {
      List<List<Long>> slotRanges = getSlotRanges(jedis);
      ImmutableList.Builder hashTags = ImmutableList.builder();
      for (List<Long> slotRange : slotRanges) {
        // we can use any slot that is in range for the node.
        // in this case, we will use the first slot.
        hashTags.add(RedisSlotToHash.correlate(slotRange.get(0)));
      }
      return hashTags.build();
    } catch (JedisException e) {
      return ImmutableList.of();
    }
  }

  /**
   * @brief Get a list of evenly distributing hashtags for the provided redis cluster.
   * @details Each hashtag will map to a slot on a different node.
   * @param jedis An established jedis client.
   * @param prefix The prefix to include as part of hashtags.
   * @return Hashtags that will each has to a slot on a different node.
   * @note Suggested return identifier: hashtags.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static List<String> getEvenlyDistributedHashesWithPrefix(
      JedisCluster jedis, String prefix) {
    try {
      List<List<Long>> slotRanges = getSlotRanges(jedis);
      ImmutableList.Builder hashTags = ImmutableList.builder();
      for (List<Long> slotRange : slotRanges) {
        // we can use any slot that is in range for the node.
        // in this case, we will use the first slot.
        hashTags.add(
            RedisSlotToHash.correlateRangeWithPrefix(slotRange.get(0), slotRange.get(1), prefix));
      }
      return hashTags.build();
    } catch (JedisException e) {
      return ImmutableList.of();
    }
  }

  /**
   * @brief Get a list of slot ranges for each of the nodes in the cluster.
   * @details This information can be found from any of the redis nodes in the cluster.
   * @param jedis An established jedis client.
   * @return Slot ranges for all of the nodes in the cluster.
   * @note Suggested return identifier: slotRanges.
   */
  @SuppressWarnings("unchecked")
  private static List<List<Long>> getSlotRanges(JedisCluster jedis) {
    // get slot information for each node
    List<Object> slots = getClusterSlots(jedis);

    // convert slot information into a list of slot ranges
    ImmutableList.Builder<List<Long>> slotRanges = ImmutableList.builder();
    for (Object slotInfoObj : slots) {
      List<Object> slotInfo = (List<Object>) slotInfoObj;
      List<Long> slotNums = slotInfoToSlotRange(slotInfo);
      slotRanges.add(slotNums);
    }

    return slotRanges.build();
  }

  /**
   * @brief Convert a jedis slotInfo object to a range or slot numbers.
   * @details Every redis node has a range of slots represented as integers.
   * @param slotInfo Slot info objects from a redis node.
   * @return The slot number range for the particular redis node.
   * @note Suggested return identifier: slotRange.
   */
  private static List<Long> slotInfoToSlotRange(List<Object> slotInfo) {
    return ImmutableList.of((Long) slotInfo.get(0), (Long) slotInfo.get(1));
  }

  /**
   * @brief Query slot information for each redis node.
   * @details Obtains cluster information for understanding slot ranges for balancing.
   * @param jedis An established jedis client.
   * @return Cluster slot information.
   * @note Suggested return identifier: clusterSlots.
   */
  private static List<Object> getClusterSlots(JedisCluster jedis) {
    JedisException nodeException = null;
    for (Map.Entry<String, JedisPool> node : jedis.getClusterNodes().entrySet()) {
      JedisPool pool = node.getValue();
      try (Jedis resource = pool.getResource()) {
        return resource.clusterSlots();
      } catch (JedisException e) {
        nodeException = e;
        // log error with node
      }
    }
    if (nodeException != null) {
      throw nodeException;
    }
    throw new JedisNoReachableClusterNodeException("No reachable node in cluster");
  }
}
