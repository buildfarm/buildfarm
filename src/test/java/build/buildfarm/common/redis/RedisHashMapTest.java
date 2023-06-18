// Copyright 2022 The Bazel Authors. All rights reserved.
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

import build.buildfarm.common.config.BuildfarmConfigs;
import build.buildfarm.instance.shard.JedisClusterFactory;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import redis.clients.jedis.JedisCluster;

/**
 * @class RedisHashMapTest
 * @brief A redis hashmap.
 * @details A redis hashmap is an implementation of a map data structure which internally uses redis
 *     to store and distribute the data. Its important to know that the lifetime of the map persists
 *     before and after the map data structure is created (since it exists in redis). Therefore, two
 *     redis maps with the same name, would in fact be the same underlying redis map.
 */
@RunWith(JUnit4.class)
public class RedisHashMapTest {
  private BuildfarmConfigs configs = BuildfarmConfigs.getInstance();
  private JedisCluster redis;

  @Before
  public void setUp() throws Exception {
    configs.getBackplane().setRedisUri("redis://localhost:6379");
    redis = JedisClusterFactory.createTest();
  }

  @After
  public void tearDown() {
    redis.close();
  }

  // Function under test: RedisHashMap
  // Reason for testing: the container can be constructed with a valid name.
  // Failure explanation: the container is throwing an exception upon construction
  @Test
  public void redisPriorityQueueConstructsWithoutError() throws Exception {
    // ACT
    new RedisHashMap("test");
  }

  // Function under test: insert & keys
  // Reason for testing: elements can be inserted and keys received
  // Failure explanation: inserting or fetching keys does not work as expected
  @Test
  public void redisInsertAndGetKeys() throws Exception {
    // ARRANGE
    RedisHashMap map = new RedisHashMap("test");

    // ACT
    map.insert(redis, "key1", "value1");
    map.insert(redis, "key2", "value2");
    map.insert(redis, "key3", "value3");

    Set<String> expected = new HashSet<>();
    expected.add("key1");
    expected.add("key2");
    expected.add("key3");

    // ASSERT
    Set<String> keys = map.keys(redis);
    assertThat(keys.equals(expected)).isTrue();
  }

  // Function under test: insert & asMap
  // Reason for testing: elements can be inserted and received back as a java map
  // Failure explanation: inserting or fetching elements does not work as expected
  @Test
  public void redisInsertAndAsMap() throws Exception {
    // ARRANGE
    RedisHashMap map = new RedisHashMap("test");

    // ACT
    map.insert(redis, "key1", "value1");
    map.insert(redis, "key2", "value2");
    map.insert(redis, "key3", "value3");

    Map<String, String> expected = new HashMap<>();
    expected.put("key1", "value1");
    expected.put("key2", "value2");
    expected.put("key3", "value3");

    // ASSERT
    Map<String, String> elements = map.asMap(redis);
    assertThat(elements.equals(expected)).isTrue();
  }

  // Function under test: remove
  // Reason for testing: elements can be removed
  // Failure explanation: the selected element was not removed
  @Test
  public void redisElementRemoved() throws Exception {
    // ARRANGE
    RedisHashMap map = new RedisHashMap("test");

    // ACT
    map.insert(redis, "key1", "value1");
    map.insert(redis, "key2", "value2");
    map.insert(redis, "key3", "value3");

    map.remove(redis, "key2");

    Map<String, String> expected = new HashMap<>();
    expected.put("key1", "value1");
    expected.put("key3", "value3");

    // ASSERT
    Map<String, String> elements = map.asMap(redis);
    assertThat(elements.equals(expected)).isTrue();
  }

  // Function under test: insertIfMissing
  // Reason for testing: element is inserted for the first time.
  // Failure explanation: Element is not inserted for the first time.
  @Test
  public void redisInsertWasMissing() throws Exception {
    // ARRANGE
    RedisHashMap map = new RedisHashMap("test");

    // ACT
    boolean wasAdded = map.insertIfMissing(redis, "key1", "value1");

    Map<String, String> expected = new HashMap<>();
    expected.put("key1", "value1");

    // ASSERT
    Map<String, String> elements = map.asMap(redis);
    assertThat(elements.equals(expected)).isTrue();
    assertThat(wasAdded).isTrue();
  }

  // Function under test: insertIfMissing
  // Reason for testing: element is not inserted the second time.
  // Failure explanation: Element is not inserted for the second time.
  @Test
  public void redisInsertWasNotMissing() throws Exception {
    // ARRANGE
    RedisHashMap map = new RedisHashMap("test");

    // ACT
    map.insertIfMissing(redis, "key1", "value1");
    boolean wasAdded = map.insertIfMissing(redis, "key1", "value1");

    Map<String, String> expected = new HashMap<>();
    expected.put("key1", "value1");

    // ASSERT
    Map<String, String> elements = map.asMap(redis);
    assertThat(elements.equals(expected)).isTrue();
    assertThat(wasAdded).isFalse();
  }

  // Function under test: exists
  // Reason for testing: exists fails as expected
  // Failure explanation: Exist did not fail but it should have
  @Test
  public void redisExistFails() throws Exception {
    // ARRANGE
    RedisHashMap map = new RedisHashMap("test");

    // ACT
    boolean exists = map.exists(redis, "key");

    // ASSERT
    assertThat(exists).isFalse();
  }

  // Function under test: exists
  // Reason for testing: exists succeeds as expected
  // Failure explanation: Exist did not succeed but it should have
  @Test
  public void redisExistSucceeds() throws Exception {
    // ARRANGE
    RedisHashMap map = new RedisHashMap("test");
    map.insert(redis, "key", "value");

    // ACT
    boolean exists = map.exists(redis, "key");

    // ASSERT
    assertThat(exists).isTrue();
  }

  // Function under test: size
  // Reason for testing: size grows as elements are added.
  // Failure explanation: Size is not growing as expected
  @Test
  public void redisSizeGrowth() throws Exception {
    // ARRANGE
    RedisHashMap map = new RedisHashMap("test");

    // ACT
    map.insert(redis, "key1", "value1");

    // ASSERT
    assertThat(map.size(redis)).isEqualTo(1);

    // ACT
    map.insert(redis, "key2", "value2");

    // ASSERT
    assertThat(map.size(redis)).isEqualTo(2);

    // ACT
    map.insert(redis, "key3", "value3");

    // ASSERT
    assertThat(map.size(redis)).isEqualTo(3);
  }

  // Function under test: remove
  // Reason for testing: Test that remove removes multiple items.
  // Failure explanation: Elements are not being removed.
  @Test
  public void redisRemoveAll() throws Exception {
    // ARRANGE
    RedisHashMap map = new RedisHashMap("test");
    Map<String, String> expected = new HashMap<>();
    expected.put("key1", "value1");
    expected.put("key4", "value4");

    // ACT
    map.insert(redis, "key1", "value1");
    map.insert(redis, "key2", "value2");
    map.insert(redis, "key3", "value3");
    map.insert(redis, "key4", "value4");
    Iterable<String> removals = Arrays.asList("key2", "key3");
    map.remove(redis, removals);

    // ASSERT
    Map<String, String> elements = map.asMap(redis);
    assertThat(elements.equals(expected)).isTrue();
  }

  @Test
  public void redisMget() {
    RedisHashMap map = new RedisHashMap("test");
    map.insert(redis, "key1", "value1");
    map.insert(redis, "key2", "value2");
    map.insert(redis, "key3", "value3");
    map.insert(redis, "key4", "value4");

    Iterable<String> fields = Arrays.asList("key2", "key3");
    List<String> expected = Arrays.asList("value2", "value3");

    assertThat(map.mget(redis, fields)).containsExactlyElementsIn(expected);
  }
}
