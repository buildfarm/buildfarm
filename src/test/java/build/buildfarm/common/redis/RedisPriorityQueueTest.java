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

import build.buildfarm.common.StringVisitor;
import build.buildfarm.common.config.BuildfarmConfigs;
import build.buildfarm.instance.shard.JedisClusterFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import redis.clients.jedis.JedisCluster;

/**
 * @class RedisPriorityQueueTest
 * @brief tests A redis queue.
 * @details A redis queue is an implementation of a queue data structure which internally uses redis
 *     to store and distribute the data. Its important to know that the lifetime of the queue
 *     persists before and after the queue data structure is created (since it exists in redis).
 *     Therefore, two redis queues with the same name, would in fact be the same underlying redis
 *     queue.
 */
@RunWith(JUnit4.class)
public class RedisPriorityQueueTest {
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

  // Function under test: RedisPriorityQueue
  // Reason for testing: the queue can be constructed with a valid cluster instance and name
  // Failure explanation: the queue is throwing an exception upon construction
  @Test
  public void redisPriorityQueueConstructsWithoutError() throws Exception {
    // ACT
    new RedisPriorityQueue("test");
  }

  // Function under test: push
  // Reason for testing: the queue can have a value pushed onto it
  // Failure explanation: the queue is throwing an exception upon push
  @Test
  public void pushPushWithoutError() throws Exception {
    // ARRANGE
    RedisPriorityQueue queue = new RedisPriorityQueue("test");

    // ACT
    queue.push(redis, "foo");
  }

  // Function under test: push
  // Reason for testing: the queue can have the different values pushed onto it
  // Failure explanation: the queue is throwing an exception upon pushing different values
  @Test
  public void pushPushDifferentWithoutError() throws Exception {
    // ARRANGE
    RedisPriorityQueue queue = new RedisPriorityQueue("test");

    // ACT
    queue.push(redis, "foo");
    queue.push(redis, "bar");
  }

  // Function under test: push
  // Reason for testing: the queue can have the same values pushed onto it
  // Failure explanation: the queue is throwing an exception upon pushing the same values
  @Test
  public void pushPushSameWithoutError() throws Exception {
    // ARRANGE
    RedisPriorityQueue queue = new RedisPriorityQueue("test");

    // ACT
    queue.push(redis, "foo");
    queue.push(redis, "foo");
  }

  // Function under test: push
  // Reason for testing: the queue can have many values pushed into it
  // Failure explanation: the queue is throwing an exception upon pushing many values
  @Test
  public void pushPushMany() throws Exception {
    // ARRANGE
    RedisPriorityQueue queue = new RedisPriorityQueue("test");

    // ACT
    for (int i = 0; i < 1000; ++i) {
      queue.push(redis, "foo" + i);
    }
  }

  // Function under test: push
  // Reason for testing: the queue size increases as elements are pushed
  // Failure explanation: the queue size is not accurately reflecting the pushes
  @Test
  public void pushPushIncreasesSize() throws Exception {
    // ARRANGE
    RedisPriorityQueue queue = new RedisPriorityQueue("test");

    // ACT / ASSERT
    assertThat(queue.size(redis)).isEqualTo(0);
    queue.push(redis, "foo");
    assertThat(queue.size(redis)).isEqualTo(1);
    queue.push(redis, "foo1");
    assertThat(queue.size(redis)).isEqualTo(2);
    queue.push(redis, "foo2");
    assertThat(queue.size(redis)).isEqualTo(3);
    queue.push(redis, "foo3");
    assertThat(queue.size(redis)).isEqualTo(4);
    queue.push(redis, "foo4");
    assertThat(queue.size(redis)).isEqualTo(5);
    queue.push(redis, "foo5");
    assertThat(queue.size(redis)).isEqualTo(6);
    queue.push(redis, "foo6");
    assertThat(queue.size(redis)).isEqualTo(7);
    queue.push(redis, "foo7");
    assertThat(queue.size(redis)).isEqualTo(8);
    queue.push(redis, "foo8");
    assertThat(queue.size(redis)).isEqualTo(9);
    queue.push(redis, "foo9");
    assertThat(queue.size(redis)).isEqualTo(10);
  }

  // Function under test: getName
  // Reason for testing: the name can be received
  // Failure explanation: name does not match what it should
  @Test
  public void getNameNameIsStored() throws Exception {
    // ARRANGE
    RedisPriorityQueue queue = new RedisPriorityQueue("queue_name");

    // ACT
    String name = queue.getName();

    // ASSERT
    assertThat(name).isEqualTo("queue_name");
  }

  // Function under test: getDequeueName
  // Reason for testing: the name can be received
  // Failure explanation: name does not match what it should
  @Test
  public void getDequeueNameNameIsStored() throws Exception {
    // ARRANGE
    RedisPriorityQueue queue = new RedisPriorityQueue("queue_name");

    // ACT
    String name = queue.getDequeueName();

    // ASSERT
    assertThat(name).isEqualTo("queue_name_dequeue");
  }

  // Function under test: size
  // Reason for testing: size adjusts with push and dequeue
  // Failure explanation: size is incorrectly reporting the expected queue size
  @Test
  public void sizeAdjustPushDequeue() throws Exception {
    // ARRANGE
    RedisPriorityQueue queue = new RedisPriorityQueue("test");

    // ACT / ASSERT
    assertThat(queue.size(redis)).isEqualTo(0);
    queue.push(redis, "foo");
    assertThat(queue.size(redis)).isEqualTo(1);
    queue.push(redis, "bar");
    assertThat(queue.size(redis)).isEqualTo(2);
    queue.push(redis, "baz");
    assertThat(queue.size(redis)).isEqualTo(3);
    queue.push(redis, "baz2");
    assertThat(queue.size(redis)).isEqualTo(4);
    queue.push(redis, "baz3");
    assertThat(queue.size(redis)).isEqualTo(5);
    queue.push(redis, "baz4");
    assertThat(queue.size(redis)).isEqualTo(6);
    queue.dequeue(redis, 1);
    assertThat(queue.size(redis)).isEqualTo(5);
    queue.dequeue(redis, 1);
    assertThat(queue.size(redis)).isEqualTo(4);
    queue.dequeue(redis, 1);
    assertThat(queue.size(redis)).isEqualTo(3);
    queue.dequeue(redis, 1);
    assertThat(queue.size(redis)).isEqualTo(2);
    queue.dequeue(redis, 1);
    assertThat(queue.size(redis)).isEqualTo(1);
    queue.dequeue(redis, 1);
    assertThat(queue.size(redis)).isEqualTo(0);
  }

  // Function under test: size
  // Reason for testing: size adjusts with push and dequeue
  // Failure explanation: size is incorrectly reporting the expected queue size
  @Test
  public void checkPriorityOnDequeue() throws Exception {
    // ARRANGE
    RedisPriorityQueue queue = new RedisPriorityQueue("test");
    String val;
    // ACT / ASSERT
    assertThat(queue.size(redis)).isEqualTo(0);
    queue.push(redis, "foo", 2);
    assertThat(queue.size(redis)).isEqualTo(1);
    queue.push(redis, "bar", 1);
    assertThat(queue.size(redis)).isEqualTo(2);
    queue.push(redis, "baz", 3);
    assertThat(queue.size(redis)).isEqualTo(3);
    queue.push(redis, "baz2", 1);
    assertThat(queue.size(redis)).isEqualTo(4);
    queue.push(redis, "baz3", 2);
    assertThat(queue.size(redis)).isEqualTo(5);
    queue.push(redis, "baz4", 1);
    assertThat(queue.size(redis)).isEqualTo(6);
    val = queue.dequeue(redis, 1);
    assertThat(val).isEqualTo("bar");
    assertThat(queue.size(redis)).isEqualTo(5);
    val = queue.dequeue(redis, 1);
    assertThat(val).isEqualTo("baz2");
    assertThat(queue.size(redis)).isEqualTo(4);
    val = queue.dequeue(redis, 1);
    assertThat(val).isEqualTo("baz4");
    assertThat(queue.size(redis)).isEqualTo(3);
    val = queue.dequeue(redis, 1);
    assertThat(val).isEqualTo("foo");
    assertThat(queue.size(redis)).isEqualTo(2);
    val = queue.dequeue(redis, 1);
    assertThat(val).isEqualTo("baz3");
    assertThat(queue.size(redis)).isEqualTo(1);
    val = queue.dequeue(redis, 1);
    assertThat(val).isEqualTo("baz");
    assertThat(queue.size(redis)).isEqualTo(0);
  }

  // Function under test: dequeue
  // Reason for testing: Test dequeue times out correctly
  // Failure explanation: dequeue does not spend the full time waiting for response
  @Test
  public void checkDequeueTimeout() throws Exception {
    // ARRANGE
    RedisPriorityQueue queue = new RedisPriorityQueue("test");

    Instant start = Instant.now();
    String val = queue.dequeue(redis, 1);
    Instant finish = Instant.now();

    long timeElapsed = Duration.between(start, finish).toMillis();
    assertThat(timeElapsed).isGreaterThan(1000L);
    assertThat(val).isEqualTo(null);
  }

  // Function under test: dequeue
  // Reason for testing: The queue supports negative priorities.
  // Failure explanation: negative prioritizes are not handled in the correct order.
  @Test
  public void checkNegativesInPriority() throws Exception {
    // ARRANGE
    RedisPriorityQueue queue = new RedisPriorityQueue("test");
    String val;

    // ACT / ASSERT
    queue.push(redis, "foo-6", 6);
    queue.push(redis, "foo-5", 5);
    queue.push(redis, "foo-3", 3);
    queue.push(redis, "negative-50", -50);
    queue.push(redis, "negative-1", -1);
    queue.push(redis, "foo-1", 1);
    queue.push(redis, "baz-2", 2);
    queue.push(redis, "foo-4", 4);

    val = queue.dequeue(redis, 1);
    assertThat(val).isEqualTo("negative-50");
    val = queue.dequeue(redis, 1);
    assertThat(val).isEqualTo("negative-1");
    val = queue.dequeue(redis, 1);
    assertThat(val).isEqualTo("foo-1");
    val = queue.dequeue(redis, 1);
    assertThat(val).isEqualTo("baz-2");
    val = queue.dequeue(redis, 1);
    assertThat(val).isEqualTo("foo-3");
    val = queue.dequeue(redis, 1);
    assertThat(val).isEqualTo("foo-4");
    val = queue.dequeue(redis, 1);
    assertThat(val).isEqualTo("foo-5");
    val = queue.dequeue(redis, 1);
    assertThat(val).isEqualTo("foo-6");
  }

  // Function under test: visit
  // Reason for testing: each element in the queue can be visited
  // Failure explanation: we are unable to visit each element in the queue
  @Test
  public void visitCheckVisitOfEachElement() throws Exception {
    // ARRANGE
    RedisPriorityQueue queue = new RedisPriorityQueue("test");
    queue.push(redis, "element 1");
    queue.push(redis, "element 2");
    queue.push(redis, "element 3");
    queue.push(redis, "element 4");
    queue.push(redis, "element 5");
    queue.push(redis, "element 6");
    queue.push(redis, "element 7");
    queue.push(redis, "element 8");

    // ACT
    List<String> visited = new ArrayList<>();
    StringVisitor visitor =
        new StringVisitor() {
          public void visit(String entry) {
            visited.add(entry);
          }
        };
    queue.visit(redis, visitor);

    // ASSERT
    assertThat(visited.size()).isEqualTo(8);
    assertThat(visited.contains("element 1")).isTrue();
    assertThat(visited.contains("element 2")).isTrue();
    assertThat(visited.contains("element 3")).isTrue();
    assertThat(visited.contains("element 4")).isTrue();
    assertThat(visited.contains("element 5")).isTrue();
    assertThat(visited.contains("element 6")).isTrue();
    assertThat(visited.contains("element 7")).isTrue();
    assertThat(visited.contains("element 8")).isTrue();
  }

  // Function under test: visit
  // Reason for testing: add and visit many elements
  // Failure explanation: we are unable to visit all the elements when there are many of them
  @Test
  public void visitVisitManyOverPageSize() throws Exception {
    // ARRANGE
    RedisPriorityQueue queue = new RedisPriorityQueue("test");
    for (int i = 0; i < 2500; ++i) {
      queue.push(redis, "foo" + i);
    }

    // ACT
    List<String> visited = new ArrayList<>();
    StringVisitor visitor =
        new StringVisitor() {
          public void visit(String entry) {
            visited.add(entry);
          }
        };
    queue.visit(redis, visitor);

    // ASSERT
    assertThat(visited.size()).isEqualTo(2500);
    for (int i = 0; i < 2500; ++i) {
      assertThat(visited.contains("foo" + i)).isTrue();
    }
  }
}
