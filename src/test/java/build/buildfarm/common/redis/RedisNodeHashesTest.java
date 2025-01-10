// Copyright 2020 The Buildfarm Authors. All rights reserved.
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

import build.buildfarm.instance.shard.JedisClusterFactory;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import redis.clients.jedis.JedisCluster;

/**
 * @class RedisNodeHashesTest
 * @brief tests A list of redis hashtags that each map to different nodes in the cluster.
 * @details When looking to evenly distribute keys across nodes, specific hashtags need obtained in
 *     which each hashtag hashes to a particular slot owned by a particular worker. This class is
 *     used to obtain the hashtags needed to hit every node in the cluster.
 */
@RunWith(JUnit4.class)
public class RedisNodeHashesTest {
  // Function under test: getEvenlyDistributedHashes
  // Reason for testing: an established redis cluster can be used to obtain distributed hashes
  // Failure explanation: there is an error in the cluster's ability to report slot ranges or
  // convert ranges to hashtags
  @Test
  public void getEvenlyDistributedHashesCanRetrieveDistributedHashes() throws Exception {
    // ARRANGE
    JedisCluster redis = JedisClusterFactory.createTest();

    // ACT
    List<String> hashtags = RedisNodeHashes.getEvenlyDistributedHashes(redis);

    // ASSERT
    assertThat(hashtags.isEmpty()).isFalse();
  }

  // Function under test: getEvenlyDistributedHashesWithPrefix
  // Reason for testing: an established redis cluster can be used to obtain distributed hashes
  // Failure explanation: there is an error in the cluster's ability to report slot ranges or
  // convert ranges to hashtags
  @Test
  public void getEvenlyDistributedHashesWithPrefixCanRetrieveDistributedHashes() throws Exception {
    // ARRANGE
    JedisCluster redis = JedisClusterFactory.createTest();

    // ACT
    List<String> hashtags = RedisNodeHashes.getEvenlyDistributedHashes(redis);

    // ASSERT
    assertThat(hashtags.isEmpty()).isFalse();
  }
}
