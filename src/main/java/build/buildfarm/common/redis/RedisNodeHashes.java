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

import static com.google.common.collect.Iterables.transform;
import static redis.clients.jedis.Protocol.CLUSTER_HASHSLOTS;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import redis.clients.jedis.ConnectionPool;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.exceptions.JedisClusterOperationException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.resps.ClusterShardInfo;

/**
 * @class RedisNodeHashes
 * @brief A list of redis hashtags that each map to different nodes in the cluster.
 * @details When looking to evenly distribute keys across nodes, specific hashtags need obtained in
 *     which each hashtag hashes to a particular slot owned by a particular worker. This class is
 *     used to obtain the hashtags needed to hit every node in the cluster.
 */
public class RedisNodeHashes {
  private static final Iterable<List<List<Long>>> SINGLETON_NODE_SLOT_RANGES =
      ImmutableList.of(ImmutableList.of(ImmutableList.of(0l, CLUSTER_HASHSLOTS - 1l)));

  /**
   * @brief Get a list of evenly distributing hashtags for the provided redis cluster.
   * @details Each hashtag will map to a slot on a different node.
   * @param jedis An established jedis client.
   * @return Hashtags that will each has to a slot on a different node.
   * @note Suggested return identifier: hashtags.
   */
  public static List<String> getEvenlyDistributedHashes(UnifiedJedis jedis) {
    if (jedis instanceof JedisCluster) {
      try {
        Iterable<List<List<Long>>> nodeSlotRanges = getNodeSlotRanges(jedis);
        ImmutableList.Builder hashTags = ImmutableList.builder();
        for (List<List<Long>> slotRanges : nodeSlotRanges) {
          // we can use any slot that is in range for the node.
          // in this case, we will use the first slot in the first range.
          hashTags.add(RedisSlotToHash.correlate(slotRanges.get(0).get(0)));
        }
        return hashTags.build();
      } catch (JedisException e) {
        return ImmutableList.of();
      }
    } else {
      return ImmutableList.of("");
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
  public static List<String> getEvenlyDistributedHashesWithPrefix(
      UnifiedJedis jedis, String prefix) {
    if (jedis instanceof JedisCluster cluster) {
      Iterable<List<List<Long>>> nodeSlotRanges = getNodeSlotRanges(cluster);
      try {
        ImmutableList.Builder hashTags = ImmutableList.builder();
        for (List<List<Long>> slotRanges : nodeSlotRanges) {
          // we can use any slot that is in range for the node.
          // in this case, we will use the first slot.
          hashTags.add(RedisSlotToHash.correlateRangesWithPrefix(slotRanges, prefix));
        }
        return hashTags.build();
      } catch (JedisException e) {
        return ImmutableList.of();
      }
    } else {
      return ImmutableList.of(prefix);
    }
  }

  /**
   * @brief Get a list of slot ranges for each of the nodes in the cluster.
   * @details This information can be found from any of the redis nodes in the cluster.
   * @param jedis An established jedis client.
   * @return Slot ranges for all of the nodes in the cluster.
   * @note Suggested return identifier: nodeSlotRanges.
   */
  private static Iterable<List<List<Long>>> getNodeSlotRanges(UnifiedJedis jedis) {
    if (jedis instanceof JedisCluster) {
      // get slot range information for each shard
      return transform(getClusterShards((JedisCluster) jedis), ClusterShardInfo::getSlots);
    } else {
      return SINGLETON_NODE_SLOT_RANGES;
    }
  }

  /**
   * @brief Query shard information from any redis node.
   * @details Obtains cluster information for understanding slot ranges for balancing.
   * @param jedis An established jedis client.
   * @return Cluster slot information.
   * @note Suggested return identifier: clusterShards.
   */
  private static List<ClusterShardInfo> getClusterShards(JedisCluster jedis) {
    JedisException nodeException = null;
    for (Map.Entry<String, ConnectionPool> node : jedis.getClusterNodes().entrySet()) {
      ConnectionPool pool = node.getValue();
      try (Jedis resource = new Jedis(pool.getResource())) {
        return resource.clusterShards();
      } catch (JedisException e) {
        nodeException = e;
        // log error with node
      }
    }
    if (nodeException != null) {
      throw nodeException;
    }
    throw new JedisClusterOperationException("No reachable node in cluster");
  }
}
