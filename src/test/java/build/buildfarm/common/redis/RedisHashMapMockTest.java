// Copyright 2022 The Buildfarm Authors. All rights reserved.
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

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import redis.clients.jedis.JedisCluster;

/**
 * @class RedisHashMapMockTest
 * @brief A redis hashmap.
 * @details A redis hashmap is an implementation of a map data structure which internally uses redis
 *     to store and distribute the data. Its important to know that the lifetime of the map persists
 *     before and after the map data structure is created (since it exists in redis). Therefore, two
 *     redis maps with the same name, would in fact be the same underlying redis map.
 */
@RunWith(JUnit4.class)
public class RedisHashMapMockTest {
  @Mock private JedisCluster redis;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  // Function under test: RedisHashMap
  // Reason for testing: the container can be constructed with a valid name.
  // Failure explanation: the container is throwing an exception upon construction
  @Test
  public void redisPriorityQueueConstructsWithoutError() throws Exception {
    // ACT
    new RedisHashMap("test");
  }

  // Function under test: insert
  // Reason for testing: elements can be inserted into the map
  // Failure explanation: inserting keys not call the expected implementation.
  @Test
  public void redisInsert() throws Exception {
    // ARRANGE
    RedisHashMap map = new RedisHashMap("test");

    // ACT
    map.insert(redis, "key1", "value1");
    map.insert(redis, "key2", "value2");
    map.insert(redis, "key3", "value3");

    // ASSERT
    verify(redis, times(1)).hset("test", "key1", "value1");
    verify(redis, times(1)).hset("test", "key2", "value2");
    verify(redis, times(1)).hset("test", "key3", "value3");
  }

  // Function under test: remove
  // Reason for testing: element can be removed by key
  // Failure explanation: removing keys does not call the expected implementation.
  @Test
  public void redisRemove() throws Exception {
    // ARRANGE
    RedisHashMap map = new RedisHashMap("test");

    // ACT
    map.insert(redis, "key1", "value1");
    map.remove(redis, "key1");

    // ASSERT
    verify(redis, times(1)).hset("test", "key1", "value1");
    verify(redis, times(1)).hdel("test", "key1");
  }

  // Function under test: keys & asMap
  // Reason for testing: representations of the map can be fetched.
  // Failure explanation: getting representations does not call the expected implementation.
  @Test
  public void redisRepresentation() throws Exception {
    // ARRANGE
    RedisHashMap map = new RedisHashMap("test");

    // ACT
    map.keys(redis);
    map.asMap(redis);

    // ASSERT
    verify(redis, times(1)).hkeys("test");
    verify(redis, times(1)).hgetAll("test");
  }

  // Function under test: insertIfMissing
  // Reason for testing: the correct implementation is used.
  // Failure explanation: Unexpected implementation used
  @Test
  public void redisInsertIfMissing() throws Exception {
    // ARRANGE
    RedisHashMap map = new RedisHashMap("test");

    // ACT
    map.insertIfMissing(redis, "key", "value");
    map.insertIfMissing(redis, "key", "value");

    // ASSERT
    verify(redis, times(2)).hsetnx("test", "key", "value");
  }

  // Function under test: exists
  // Reason for testing: the correct implementation is used.
  // Failure explanation: Unexpected implementation used
  @Test
  public void redisExists() throws Exception {
    // ARRANGE
    RedisHashMap map = new RedisHashMap("test");

    // ACT
    map.exists(redis, "key");

    // ASSERT
    verify(redis, times(1)).hexists("test", "key");
  }

  // Function under test: size
  // Reason for testing: the correct implementation is used.
  // Failure explanation: Unexpected implementation used
  @Test
  public void redisSize() throws Exception {
    // ARRANGE
    RedisHashMap map = new RedisHashMap("test");

    // ACT
    map.size(redis);

    // ASSERT
    verify(redis, times(1)).hlen("test");
  }
}
