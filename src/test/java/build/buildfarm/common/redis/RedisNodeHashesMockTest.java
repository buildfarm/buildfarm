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

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

/**
 * @class RedisNodeHashesMockTest
 * @brief tests A list of redis hashtags that each map to different nodes in the cluster.
 * @details When looking to evenly distribute keys across nodes, specific hashtags need obtained in
 *     which each hashtag hashes to a particular slot owned by a particular worker. This class is
 *     used to obtain the hashtags needed to hit every node in the cluster.
 */
@RunWith(JUnit4.class)
public class RedisNodeHashesMockTest {
  // Function under test: getEvenlyDistributedHashes
  // Reason for testing: an established redis cluster can be used to obtain distributed hashes
  // Failure explanation: there is an error in the cluster's ability to report slot ranges or
  // convert ranges to hashtags
  @Test
  public void getEvenlyDistributedHashesCanRetrieveDistributedHashes() throws Exception {
    // ARRANGE
    Jedis node = mock(Jedis.class);
    when(node.clusterSlots()).thenReturn(Collections.singletonList(Arrays.asList(0L, 100L)));

    JedisPool pool = mock(JedisPool.class);
    when(pool.getResource()).thenReturn(node);

    JedisCluster redis = mock(JedisCluster.class);
    Map<String, JedisPool> poolMap = new HashMap<>();
    poolMap.put("key1", pool);
    when(redis.getClusterNodes()).thenReturn(poolMap);

    // ACT
    List<String> hashtags = RedisNodeHashes.getEvenlyDistributedHashes(redis);

    // ASSERT
    assertThat(hashtags.isEmpty()).isFalse();
  }

  // Function under test: getEvenlyDistributedHashes
  // Reason for testing: without mocking we exercise the fallback behavior
  // Failure explanation: the fallback behavior does not result in successful completion of call
  @Test
  public void getEvenlyDistributedHashesCannotRetrieveDistributedHashes() throws Exception {
    // ARRANGE
    JedisCluster redis = mock(JedisCluster.class);

    // ACT
    List<String> hashtags = RedisNodeHashes.getEvenlyDistributedHashes(redis);

    // ASSERT
    assertThat(hashtags.isEmpty()).isTrue();
  }

  // Function under test: getEvenlyDistributedHashes
  // Reason for testing: the object can be constructed
  // Failure explanation: the object cannot be constructed
  @Test
  public void getEvenlyDistributedHashesCanConstruct() throws Exception {
    new RedisNodeHashes();
  }

  // Function under test: getEvenlyDistributedHashesWithPrefix
  // Reason for testing: an established redis cluster can be used to obtain distributed hashes with
  // prefixes
  // Failure explanation: there is an error in the cluster's ability to report slot ranges or
  // convert ranges to hashtags
  @Test
  public void getEvenlyDistributedHashesWithPrefixExpectedPrefixHashes() throws Exception {
    // ARRANGE
    Jedis node = mock(Jedis.class);
    when(node.clusterSlots())
        .thenReturn(Arrays.asList(Arrays.asList(0L, 100L), Arrays.asList(101L, 200L)));

    JedisPool pool = mock(JedisPool.class);
    when(pool.getResource()).thenReturn(node);

    JedisCluster redis = mock(JedisCluster.class);
    Map<String, JedisPool> poolMap = new HashMap<>();
    poolMap.put("key1", pool);
    when(redis.getClusterNodes()).thenReturn(poolMap);

    // ACT
    List<String> hashtags =
        RedisNodeHashes.getEvenlyDistributedHashesWithPrefix(redis, "Execution");

    // ASSERT
    assertThat(hashtags.size()).isEqualTo(2);
    assertThat(hashtags.get(0)).isEqualTo("Execution:97");
    assertThat(hashtags.get(1)).isEqualTo("Execution:66");
  }
}
