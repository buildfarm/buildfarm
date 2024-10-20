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
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import build.buildfarm.common.StringVisitor;
import build.buildfarm.common.config.BuildfarmConfigs;
import build.buildfarm.instance.shard.JedisClusterFactory;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.UnifiedJedis;

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
  private JedisPooled pooled;
  private Jedis redis;

  @Before
  public void setUp() throws Exception {
    configs.getBackplane().setRedisUri("redis://localhost:6379");
    UnifiedJedis unified = JedisClusterFactory.createTest();
    assertThat(unified).isInstanceOf(JedisPooled.class);
    pooled = (JedisPooled) unified;
    redis = new Jedis(pooled.getPool().getResource());
  }

  @After
  public void tearDown() {
    redis.close();
    pooled.close();
  }

  // Function under test: RedisPriorityQueue
  // Reason for testing: the queue can be constructed with a valid cluster instance and name
  // Failure explanation: the queue throws an exception upon construction
  @Test
  public void redisPriorityQueueConstructsWithoutError() throws Exception {
    // ACT
    new RedisPriorityQueue(redis, "test");
  }

  // Function under test: offer
  // Reason for testing: the queue can have a value offered to it
  // Failure explanation: the queue throws an exception upon offer
  @Test
  public void offerWithoutError() throws Exception {
    // ARRANGE
    RedisPriorityQueue queue = new RedisPriorityQueue(redis, "test");

    // ACT
    queue.offer("foo");
  }

  // Function under test: offer
  // Reason for testing: the queue can have the different values offered to it
  // Failure explanation: the queue throws an exception upon offering different values
  @Test
  public void offerDifferentWithoutError() throws Exception {
    // ARRANGE
    RedisPriorityQueue queue = new RedisPriorityQueue(redis, "test");

    // ACT
    queue.offer("foo");
    queue.offer("bar");
  }

  // Function under test: offer
  // Reason for testing: the queue can have the same values offered to it
  // Failure explanation: the queue throws an exception upon offering the same values
  @Test
  public void pushPushSameWithoutError() throws Exception {
    // ARRANGE
    RedisPriorityQueue queue = new RedisPriorityQueue(redis, "test");

    // ACT
    queue.offer("foo");
    queue.offer("foo");
  }

  // Function under test: offer
  // Reason for testing: the queue can have many values offered to it
  // Failure explanation: the queue throws an exception upon offering many values
  @Test
  public void pushPushMany() throws Exception {
    // ARRANGE
    RedisPriorityQueue queue = new RedisPriorityQueue(redis, "test");

    // ACT
    for (int i = 0; i < 1000; ++i) {
      queue.offer("foo" + i);
    }
  }

  // Function under test: offer
  // Reason for testing: the queue size increases as elements are offered
  // Failure explanation: the queue size does not reflect offers
  @Test
  public void pushPushIncreasesSize() throws Exception {
    // ARRANGE
    RedisPriorityQueue queue = new RedisPriorityQueue(redis, "test");

    // ACT / ASSERT
    assertThat(queue.size()).isEqualTo(0);
    queue.offer("foo");
    assertThat(queue.size()).isEqualTo(1);
    queue.offer("foo1");
    assertThat(queue.size()).isEqualTo(2);
    queue.offer("foo2");
    assertThat(queue.size()).isEqualTo(3);
    queue.offer("foo3");
    assertThat(queue.size()).isEqualTo(4);
    queue.offer("foo4");
    assertThat(queue.size()).isEqualTo(5);
    queue.offer("foo5");
    assertThat(queue.size()).isEqualTo(6);
    queue.offer("foo6");
    assertThat(queue.size()).isEqualTo(7);
    queue.offer("foo7");
    assertThat(queue.size()).isEqualTo(8);
    queue.offer("foo8");
    assertThat(queue.size()).isEqualTo(9);
    queue.offer("foo9");
    assertThat(queue.size()).isEqualTo(10);
  }

  // Function under test: getDequeueName
  // Reason for testing: the name can be received
  // Failure explanation: name does not match what it should
  @Test
  public void getDequeueNameNameIsStored() throws Exception {
    // ARRANGE
    RedisPriorityQueue queue = new RedisPriorityQueue(redis, "queue_name");

    // ACT
    String name = queue.getDequeueName();

    // ASSERT
    assertThat(name).isEqualTo("queue_name_dequeue");
  }

  // Function under test: size
  // Reason for testing: size adjusts with offer(and dequeue
  // Failure explanation: size is incorrectly reporting the expected queue size
  @Test
  public void sizeAdjustPushDequeue() throws Exception {
    // ARRANGE
    RedisPriorityQueue queue = new RedisPriorityQueue(redis, "test");
    ExecutorService service = mock(ExecutorService.class);
    Duration timeout = Duration.ofSeconds(1);

    // ACT / ASSERT
    assertThat(queue.size()).isEqualTo(0);
    queue.offer("foo");
    assertThat(queue.size()).isEqualTo(1);
    queue.offer("bar");
    assertThat(queue.size()).isEqualTo(2);
    queue.offer("baz");
    assertThat(queue.size()).isEqualTo(3);
    queue.offer("baz2");
    assertThat(queue.size()).isEqualTo(4);
    queue.offer("baz3");
    assertThat(queue.size()).isEqualTo(5);
    queue.offer("baz4");
    assertThat(queue.size()).isEqualTo(6);
    queue.take(timeout);
    assertThat(queue.size()).isEqualTo(5);
    queue.take(timeout);
    assertThat(queue.size()).isEqualTo(4);
    queue.take(timeout);
    assertThat(queue.size()).isEqualTo(3);
    queue.take(timeout);
    assertThat(queue.size()).isEqualTo(2);
    queue.take(timeout);
    assertThat(queue.size()).isEqualTo(1);
    queue.take(timeout);
    assertThat(queue.size()).isEqualTo(0);
    verifyNoInteractions(service);
  }

  // Function under test: size
  // Reason for testing: size adjusts with offer(and take(timeout);
  // Failure explanation: size is incorrectly reporting the expected queue size
  @Test
  public void checkPriorityOnDequeue() throws Exception {
    // ARRANGE
    RedisPriorityQueue queue = new RedisPriorityQueue(redis, "test");
    ExecutorService service = mock(ExecutorService.class);
    Duration timeout = Duration.ofSeconds(1);
    // ACT / ASSERT
    assertThat(queue.size()).isEqualTo(0);
    queue.offer("prio_2_1", 2);
    assertThat(queue.size()).isEqualTo(1);
    queue.offer("prio_1_1", 1);
    assertThat(queue.size()).isEqualTo(2);
    queue.offer("prio_3_1", 3);
    assertThat(queue.size()).isEqualTo(3);
    queue.offer("prio_1_2", 1);
    assertThat(queue.size()).isEqualTo(4);
    queue.offer("prio_2_2", 2);
    assertThat(queue.size()).isEqualTo(5);
    queue.offer("prio_1_3", 1);
    assertThat(queue.size()).isEqualTo(6);
    // priority 1
    assertThat(ImmutableList.of(queue.take(timeout), queue.take(timeout), queue.take(timeout)))
        .containsExactly("prio_1_1", "prio_1_2", "prio_1_3");
    assertThat(queue.size()).isEqualTo(3);
    // priority 2
    assertThat(ImmutableList.of(queue.take(timeout), queue.take(timeout)))
        .containsExactly("prio_2_1", "prio_2_2");
    assertThat(queue.size()).isEqualTo(1);
    // priority 3
    assertThat(ImmutableList.of(queue.take(timeout))).containsExactly("prio_3_1");
    assertThat(queue.size()).isEqualTo(0);
    verifyNoInteractions(service);
  }

  // Function under test: dequeue
  // Reason for testing: Test dequeue times out correctly
  // Failure explanation: dequeue does not spend the full time waiting for response
  @Test
  public void checkDequeueTimeout() throws Exception {
    // ARRANGE
    RedisPriorityQueue queue = new RedisPriorityQueue(redis, "test");
    ExecutorService service = mock(ExecutorService.class);

    Stopwatch stopwatch = Stopwatch.createStarted();
    String val = queue.take(Duration.ofSeconds(1));
    long timeElapsed = stopwatch.elapsed(MILLISECONDS);

    assertThat(timeElapsed).isGreaterThan(1000L);
    assertThat(val).isEqualTo(null);
    verifyNoInteractions(service);
  }

  // Function under test: dequeue
  // Reason for testing: The queue supports negative priorities.
  // Failure explanation: negative prioritizes are not handled in the correct order.
  @Test
  public void checkNegativesInPriority() throws Exception {
    // ARRANGE
    RedisPriorityQueue queue = new RedisPriorityQueue(redis, "test");
    ExecutorService service = mock(ExecutorService.class);
    Duration timeout = Duration.ofSeconds(1);
    String val;

    // ACT / ASSERT
    queue.offer("foo-6", 6);
    queue.offer("foo-5", 5);
    queue.offer("foo-3", 3);
    queue.offer("negative-50", -50);
    queue.offer("negative-1", -1);
    queue.offer("foo-1", 1);
    queue.offer("baz-2", 2);
    queue.offer("foo-4", 4);

    val = queue.take(timeout);
    assertThat(val).isEqualTo("negative-50");
    val = queue.take(timeout);
    assertThat(val).isEqualTo("negative-1");
    val = queue.take(timeout);
    assertThat(val).isEqualTo("foo-1");
    val = queue.take(timeout);
    assertThat(val).isEqualTo("baz-2");
    val = queue.take(timeout);
    assertThat(val).isEqualTo("foo-3");
    val = queue.take(timeout);
    assertThat(val).isEqualTo("foo-4");
    val = queue.take(timeout);
    assertThat(val).isEqualTo("foo-5");
    val = queue.take(timeout);
    assertThat(val).isEqualTo("foo-6");
    verifyNoInteractions(service);
  }

  // Function under test: visit
  // Reason for testing: each element in the queue can be visited
  // Failure explanation: we are unable to visit each element in the queue
  @Test
  public void visitCheckVisitOfEachElement() throws Exception {
    // ARRANGE
    RedisPriorityQueue queue = new RedisPriorityQueue(redis, "test");
    queue.offer("element 1");
    queue.offer("element 2");
    queue.offer("element 3");
    queue.offer("element 4");
    queue.offer("element 5");
    queue.offer("element 6");
    queue.offer("element 7");
    queue.offer("element 8");

    // ACT
    List<String> visited = new ArrayList<>();
    StringVisitor visitor =
        new StringVisitor() {
          public void visit(String entry) {
            visited.add(entry);
          }
        };
    queue.visit(visitor);

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
    RedisPriorityQueue queue = new RedisPriorityQueue(redis, "test");
    for (int i = 0; i < 2500; ++i) {
      queue.offer("foo" + i);
    }

    // ACT
    List<String> visited = new ArrayList<>();
    StringVisitor visitor =
        new StringVisitor() {
          public void visit(String entry) {
            visited.add(entry);
          }
        };
    queue.visit(visitor);

    // ASSERT
    assertThat(visited.size()).isEqualTo(2500);
    for (int i = 0; i < 2500; ++i) {
      assertThat(visited.contains("foo" + i)).isTrue();
    }
  }
}
