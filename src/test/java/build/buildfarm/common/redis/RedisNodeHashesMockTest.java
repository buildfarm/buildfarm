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
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static redis.clients.jedis.Protocol.ClusterKeyword.SHARDS;
import static redis.clients.jedis.Protocol.Command.CLUSTER;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import redis.clients.jedis.Connection;
import redis.clients.jedis.ConnectionPool;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.resps.ClusterShardInfo;
import redis.clients.jedis.util.SafeEncoder;

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
    Connection connection = spy(Connection.class);
    doNothing().when(connection).connect();
    doNothing().when(connection).sendCommand(CLUSTER, SHARDS);
    doReturn(
            Arrays.asList(
                Arrays.asList(
                    SafeEncoder.encode(ClusterShardInfo.SLOTS), Arrays.asList(0L, 100L),
                    SafeEncoder.encode(ClusterShardInfo.NODES), Arrays.asList())))
        .when(connection)
        .getObjectMultiBulkReply();

    ConnectionPool pool = mock(ConnectionPool.class);
    when(pool.getResource()).thenReturn(connection);
    connection.setHandlingPool(pool);

    JedisCluster redis = mock(JedisCluster.class);
    Map<String, ConnectionPool> poolMap = new HashMap<>();
    poolMap.put("key1", pool);
    when(redis.getClusterNodes()).thenReturn(poolMap);

    // ACT
    List<String> hashtags = RedisNodeHashes.getEvenlyDistributedHashes(redis);

    // ASSERT
    assertThat(hashtags.isEmpty()).isFalse();
    verify(pool, times(1)).getResource();
    verify(pool, times(1)).returnResource(connection);
    verifyNoMoreInteractions(pool);
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
    Connection connection = spy(Connection.class);
    doNothing().when(connection).connect();
    doNothing().when(connection).sendCommand(CLUSTER, SHARDS);
    doReturn(
            Arrays.asList(
                Arrays.asList(
                    SafeEncoder.encode(ClusterShardInfo.SLOTS), Arrays.asList(0L, 100L),
                    SafeEncoder.encode(ClusterShardInfo.NODES), Arrays.asList()),
                Arrays.asList(
                    SafeEncoder.encode(ClusterShardInfo.SLOTS), Arrays.asList(101L, 200L),
                    SafeEncoder.encode(ClusterShardInfo.NODES), Arrays.asList())))
        .when(connection)
        .getObjectMultiBulkReply();
    doReturn(false).when(connection).isBroken();

    ConnectionPool pool = mock(ConnectionPool.class);
    when(pool.getResource()).thenReturn(connection);
    connection.setHandlingPool(pool);

    JedisCluster redis = mock(JedisCluster.class);
    Map<String, ConnectionPool> poolMap = new HashMap<>();
    poolMap.put("key1", pool);
    when(redis.getClusterNodes()).thenReturn(poolMap);

    // ACT
    List<String> hashtags =
        RedisNodeHashes.getEvenlyDistributedHashesWithPrefix(redis, "Execution");

    // ASSERT
    verify(connection, times(1)).sendCommand(CLUSTER, SHARDS);
    verify(connection, times(1)).getObjectMultiBulkReply();
    verify(connection, times(1)).close();
    verify(connection, times(1)).isBroken();
    verify(connection, times(1)).setHandlingPool(pool);
    verifyNoMoreInteractions(connection);
    assertThat(hashtags.size()).isEqualTo(2);
    assertThat(hashtags.get(0)).isEqualTo("Execution:97");
    assertThat(hashtags.get(1)).isEqualTo("Execution:66");
    verify(pool, times(1)).getResource();
    verify(pool, times(1)).returnResource(connection);
    verifyNoMoreInteractions(pool);
  }
}
