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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.params.GetExParams;

/**
 * @class RedisMapMockTest
 * @brief tests A redis map.
 * @details A redis map is an implementation of a map data structure which internally uses redis to
 *     store and distribute the data. Its important to know that the lifetime of the map persists
 *     before and after the map data structure is created (since it exists in redis). Therefore, two
 *     redis maps with the same name, would in fact be the same underlying redis map.
 */
@RunWith(JUnit4.class)
public class RedisMapMockTest {
  // Function under test: insert
  // Reason for testing: test how an element is added to a map
  // Failure explanation: jedis was not called as expected
  @Test
  public void insertInsert() throws Exception {
    // ARRANGE
    JedisCluster redis = mock(JedisCluster.class);
    RedisMap<String> map = new RedisMap<>("test", new IdentityTranslator());

    // ACT
    map.insert(redis, "key", "value", 60);

    // ASSERT
    verify(redis, times(1)).setex("test:key", 60, "value");
  }

  // Function under test: remove
  // Reason for testing: test how an element is removed to a map
  // Failure explanation: jedis was not called as expected
  @Test
  public void removeRemove() throws Exception {
    // ARRANGE
    JedisCluster redis = mock(JedisCluster.class);
    RedisMap<String> map = new RedisMap<>("test", new IdentityTranslator());

    // ACT
    map.insert(redis, "key", "value", 60);
    map.remove(redis, "key");

    // ASSERT
    verify(redis, times(1)).del("test:key");
  }

  // Function under test: get
  // Reason for testing: test how an element is looked up in a map
  // Failure explanation: jedis was not called as expected
  @Test
  public void getGet() throws Exception {
    // ARRANGE
    JedisCluster redis = mock(JedisCluster.class);
    when(redis.get("test:key")).thenReturn("value");
    RedisMap<String> map = new RedisMap<>("test", new IdentityTranslator());

    // ACT
    map.insert(redis, "key", "value", 60);
    String value = map.get(redis, "key");

    // ASSERT
    verify(redis, times(1)).get("test:key");
    assertThat(value).isEqualTo("value");
  }

  // Function under test: getex
  // Reason for testing: test how an element is looked up in a map
  // Failure explanation: jedis was not called as expected
  @Test
  public void getGetEx() throws Exception {
    // ARRANGE
    JedisCluster redis = mock(JedisCluster.class);
    GetExParams params = GetExParams.getExParams().ex(60);
    when(redis.getEx("test:key", params)).thenReturn("value");
    RedisMap<String> map = new RedisMap<>("test", new IdentityTranslator());

    // ACT
    map.insert(redis, "key", "value", 60);
    String value = map.getex(redis, "key", 60);

    // ASSERT
    verify(redis, times(1)).getEx("test:key", params);
    assertThat(value).isEqualTo("value");
  }
}
