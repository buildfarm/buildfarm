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
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;

import build.buildfarm.common.Visitor;
import build.buildfarm.common.config.BuildfarmConfigs;
import build.buildfarm.common.redis.BalancedRedisQueue.BalancedQueueEntry;
import build.buildfarm.instance.shard.JedisClusterFactory;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import redis.clients.jedis.UnifiedJedis;

/**
 * @class BalancedRedisQueueTest
 * @brief tests A balanced cluster queue.
 * @details A balanced cluster queue is an implementation of a queue data structure which internally
 *     uses multiple cluster nodes to distribute the data across the cluster. Its important to know
 *     that the lifetime of the queue persists before and after the queue data structure is created
 *     (since it exists in jedis). Therefore, two cluster queues with the same name, would in fact
 *     be the same underlying cluster queues.
 */
@RunWith(JUnit4.class)
public class BalancedRedisQueueTest {
  private BuildfarmConfigs configs = BuildfarmConfigs.getInstance();
  private UnifiedJedis jedis;

  @Before
  public void setUp() throws Exception {
    configs.getBackplane().setRedisUri("cluster://localhost:6379");
    jedis = JedisClusterFactory.createTest();
  }

  @After
  public void tearDown() {
    jedis.close();
  }

  // Function under test: BalancedRedisQueue
  // Reason for testing: the queue can be constructed with a valid cluster instance and name
  // Failure explanation: the queue is throwing an exception upon construction
  @Test
  public void balancedRedisQueueCreateHashesConstructsWithoutError() throws Exception {
    // ACT
    new BalancedRedisQueue("test", ImmutableList.of(), RedisQueue::decorate);
  }

  // Function under test: offer
  // Reason for testing: the queue can have a value pushed onto it
  // Failure explanation: the queue is throwing an exception upon offer
  @Test
  public void pushPushWithoutError() throws Exception {
    // ARRANGE
    List<String> hashtags = RedisNodeHashes.getEvenlyDistributedHashes(jedis);
    BalancedRedisQueue queue = new BalancedRedisQueue("test", hashtags, RedisQueue::decorate);

    // ACT
    queue.offer(jedis, "foo");
  }

  // Function under test: offer
  // Reason for testing: the queue can have the different values pushed onto it
  // Failure explanation: the queue is throwing an exception upon pushing different values
  @Test
  public void pushPushDifferentWithoutError() throws Exception {
    // ARRANGE
    List<String> hashtags = RedisNodeHashes.getEvenlyDistributedHashes(jedis);
    BalancedRedisQueue queue = new BalancedRedisQueue("test", hashtags, RedisQueue::decorate);

    // ACT
    queue.offer(jedis, "foo");
    queue.offer(jedis, "bar");
  }

  // Function under test: offer
  // Reason for testing: the queue can have the same values pushed onto it
  // Failure explanation: the queue is throwing an exception upon pushing the same values
  @Test
  public void pushPushSameWithoutError() throws Exception {
    // ARRANGE
    List<String> hashtags = RedisNodeHashes.getEvenlyDistributedHashes(jedis);
    BalancedRedisQueue queue = new BalancedRedisQueue("test", hashtags, RedisQueue::decorate);

    // ACT
    queue.offer(jedis, "foo");
    queue.offer(jedis, "foo");
  }

  // Function under test: offer
  // Reason for testing: the queue can have many values pushed into it
  // Failure explanation: the queue is throwing an exception upon pushing many values
  @Test
  public void pushPushMany() throws Exception {
    // ARRANGE
    List<String> hashtags = RedisNodeHashes.getEvenlyDistributedHashes(jedis);
    BalancedRedisQueue queue = new BalancedRedisQueue("test", hashtags, RedisQueue::decorate);

    // ACT
    for (int i = 0; i < 1000; ++i) {
      queue.offer(jedis, "foo" + i);
    }
  }

  // Function under test: offer
  // Reason for testing: the queue size increases as elements are pushed
  // Failure explanation: the queue size is not accurately reflecting the pushes
  @Test
  public void pushPushIncreasesSize() throws Exception {
    // ARRANGE
    List<String> hashtags = RedisNodeHashes.getEvenlyDistributedHashes(jedis);
    BalancedRedisQueue queue = new BalancedRedisQueue("test", hashtags, RedisQueue::decorate);

    // ACT / ASSERT
    assertThat(queue.size(jedis)).isEqualTo(0);
    queue.offer(jedis, "foo");
    assertThat(queue.size(jedis)).isEqualTo(1);
    queue.offer(jedis, "foo");
    assertThat(queue.size(jedis)).isEqualTo(2);
    queue.offer(jedis, "foo");
    assertThat(queue.size(jedis)).isEqualTo(3);
    queue.offer(jedis, "foo");
    assertThat(queue.size(jedis)).isEqualTo(4);
    queue.offer(jedis, "foo");
    assertThat(queue.size(jedis)).isEqualTo(5);
    queue.offer(jedis, "foo");
    assertThat(queue.size(jedis)).isEqualTo(6);
    queue.offer(jedis, "foo");
    assertThat(queue.size(jedis)).isEqualTo(7);
    queue.offer(jedis, "foo");
    assertThat(queue.size(jedis)).isEqualTo(8);
    queue.offer(jedis, "foo");
    assertThat(queue.size(jedis)).isEqualTo(9);
    queue.offer(jedis, "foo");
    assertThat(queue.size(jedis)).isEqualTo(10);
  }

  // Function under test: removeFromDequeue
  // Reason for testing: removing returns false because the queue is empty and there is nothing to
  // remove
  // Failure explanation: the queue was either not empty, or an error occured while removing from an
  // empty queue
  @Test
  public void removeFromDequeueFalseOnEmpty() throws Exception {
    // ARRANGE
    List<String> hashtags = RedisNodeHashes.getEvenlyDistributedHashes(jedis);
    BalancedRedisQueue queue = new BalancedRedisQueue("test", hashtags, RedisQueue::decorate);

    // ACT
    Boolean success = queue.removeFromDequeue(jedis, new BalancedQueueEntry("test", "foo"));

    // ASSERT
    assertThat(success).isFalse();
  }

  // Function under test: removeFromDequeue
  // Reason for testing: removing returns false because the queue does not contain the value to be
  // removed
  // Failure explanation: the queue was either contained the value or incorrectly reported a
  // deletion
  @Test
  public void removeFromDequeueFalseWhenValueIsMissing() throws Exception {
    // ARRANGE
    List<String> hashtags = RedisNodeHashes.getEvenlyDistributedHashes(jedis);
    BalancedRedisQueue queue = new BalancedRedisQueue("test", hashtags, RedisQueue::decorate);
    ExecutorService service = newSingleThreadExecutor();

    // ACT
    queue.offer(jedis, "foo");
    queue.offer(jedis, "bar");
    queue.take(jedis, service);
    queue.take(jedis, service);
    service.shutdown();
    Boolean success = queue.removeFromDequeue(jedis, new BalancedQueueEntry("test", "baz"));

    // ASSERT
    assertThat(service.awaitTermination(0, SECONDS)).isTrue();
    assertThat(success).isFalse();
  }

  // Function under test: removeFromDequeue
  // Reason for testing: removing returns true because the queue contained the value before removing
  // Failure explanation: the queue either did not contain the value or incorrectly reported a
  // deletion
  @Test
  public void removeFromDequeueTrueWhenValueExists() throws Exception {
    // ARRANGE
    List<String> hashtags = RedisNodeHashes.getEvenlyDistributedHashes(jedis);
    BalancedRedisQueue queue = new BalancedRedisQueue("test", hashtags, RedisQueue::decorate);
    ExecutorService service = newSingleThreadExecutor();

    // ACT
    queue.offer(jedis, "foo");
    queue.offer(jedis, "bar");
    queue.offer(jedis, "baz");
    queue.take(jedis, service);
    queue.take(jedis, service);
    queue.take(jedis, service);
    service.shutdown();
    Boolean success = queue.removeFromDequeue(jedis, new BalancedQueueEntry("test", "bar"));

    // ASSERT
    assertThat(service.awaitTermination(0, SECONDS)).isTrue();
    assertThat(success).isTrue();
  }

  // Function under test: getName
  // Reason for testing: the name can be received
  // Failure explanation: name does not match what it should
  @Test
  public void getNameNameIsStored() throws Exception {
    // ARRANGE
    List<String> hashtags = RedisNodeHashes.getEvenlyDistributedHashes(jedis);
    BalancedRedisQueue queue = new BalancedRedisQueue("queue_name", hashtags, RedisQueue::decorate);

    // ACT
    String name = queue.getName();

    // ASSERT
    assertThat(name).isEqualTo("queue_name");
  }

  // Function under test: getName
  // Reason for testing: the name is stored without a hashtag
  // Failure explanation: name does not match what it should
  @Test
  public void getNameNameHasHashtagRemovedFront() throws Exception {
    // ARRANGE
    List<String> hashtags = RedisNodeHashes.getEvenlyDistributedHashes(jedis);
    BalancedRedisQueue queue =
        new BalancedRedisQueue("{hash}queue_name", hashtags, RedisQueue::decorate);
    // ACT
    String name = queue.getName();

    // ASSERT
    assertThat(name).isEqualTo("queue_name");
  }

  // Function under test: getName
  // Reason for testing: the name is stored without a hashtag
  // Failure explanation: name does not match what it should
  @Test
  public void getNameNameHasHashtagRemovedFrontPriority() throws Exception {
    // ARRANGE
    List<String> hashtags = RedisNodeHashes.getEvenlyDistributedHashes(jedis);
    BalancedRedisQueue queue =
        new BalancedRedisQueue("{hash}queue_name", hashtags, RedisPriorityQueue::decorate);
    // ACT
    String name = queue.getName();

    // ASSERT
    assertThat(name).isEqualTo("queue_name");
  }

  // Function under test: getName
  // Reason for testing: the name is stored without a hashtag
  // Failure explanation: name does not match what it should
  @Test
  public void getNameNameHasHashtagColonRemovedFront() throws Exception {
    // ARRANGE
    List<String> hashtags = RedisNodeHashes.getEvenlyDistributedHashes(jedis);
    // similar to what has been seen in configuration files
    BalancedRedisQueue queue =
        new BalancedRedisQueue("{Execution}:QueuedOperations", hashtags, RedisQueue::decorate);
    // ACT
    String name = queue.getName();

    // ASSERT
    assertThat(name).isEqualTo(":QueuedOperations");
  }

  // Function under test: getName
  // Reason for testing: the name is stored without a hashtag
  // Failure explanation: name does not match what it should
  @Test
  public void getNameNameHasHashtagColonRemovedFrontPriority() throws Exception {
    // ARRANGE
    List<String> hashtags = RedisNodeHashes.getEvenlyDistributedHashes(jedis);
    // similar to what has been seen in configuration files
    BalancedRedisQueue queue =
        new BalancedRedisQueue(
            "{Execution}:QueuedOperations", hashtags, RedisPriorityQueue::decorate);
    // ACT
    String name = queue.getName();

    // ASSERT
    assertThat(name).isEqualTo(":QueuedOperations");
  }

  // Function under test: getName
  // Reason for testing: the name is stored without a hashtag
  // Failure explanation: name does not match what it should
  @Test
  public void getNameNameHasHashtagRemovedBack() throws Exception {
    // ARRANGE
    List<String> hashtags = RedisNodeHashes.getEvenlyDistributedHashes(jedis);
    BalancedRedisQueue queue =
        new BalancedRedisQueue("queue_name{hash}", hashtags, RedisQueue::decorate);
    // ACT
    String name = queue.getName();

    // ASSERT
    assertThat(name).isEqualTo("queue_name");
  }

  // Function under test: getName
  // Reason for testing: the name is stored without a hashtag
  // Failure explanation: name does not match what it should
  @Test
  public void getNameNameHasHashtagRemovedMiddle() throws Exception {
    // ARRANGE
    List<String> hashtags = RedisNodeHashes.getEvenlyDistributedHashes(jedis);
    BalancedRedisQueue queue =
        new BalancedRedisQueue("queue_{hash}name", hashtags, RedisQueue::decorate);
    // ACT
    String name = queue.getName();

    // ASSERT
    assertThat(name).isEqualTo("queue_name");
  }

  // Function under test: getName
  // Reason for testing: the name is stored without a hashtag
  // Failure explanation: name does not match what it should
  @Test
  public void getNameNameHasHashtagRemovedFrontBack() throws Exception {
    // ARRANGE
    List<String> hashtags = RedisNodeHashes.getEvenlyDistributedHashes(jedis);
    BalancedRedisQueue queue =
        new BalancedRedisQueue("{hash}queue_name{hash}", hashtags, RedisQueue::decorate);
    // ACT
    String name = queue.getName();

    // ASSERT
    assertThat(name).isEqualTo("queue_name");
  }

  // Function under test: size
  // Reason for testing: size adjusts with offer and take
  // Failure explanation: size is incorrectly reporting the expected queue size
  @Test
  public void sizeAdjustPushPop() throws Exception {
    // ARRANGE
    List<String> hashtags = RedisNodeHashes.getEvenlyDistributedHashes(jedis);
    BalancedRedisQueue queue = new BalancedRedisQueue("test", hashtags, RedisQueue::decorate);
    ExecutorService service = newSingleThreadExecutor();

    // ACT / ASSERT
    assertThat(queue.size(jedis)).isEqualTo(0);
    queue.offer(jedis, "foo");
    assertThat(queue.size(jedis)).isEqualTo(1);
    queue.offer(jedis, "bar");
    assertThat(queue.size(jedis)).isEqualTo(2);
    queue.offer(jedis, "baz");
    assertThat(queue.size(jedis)).isEqualTo(3);
    queue.offer(jedis, "baz");
    assertThat(queue.size(jedis)).isEqualTo(4);
    queue.offer(jedis, "baz");
    assertThat(queue.size(jedis)).isEqualTo(5);
    queue.offer(jedis, "baz");
    assertThat(queue.size(jedis)).isEqualTo(6);
    queue.take(jedis, service);
    assertThat(queue.size(jedis)).isEqualTo(5);
    queue.take(jedis, service);
    assertThat(queue.size(jedis)).isEqualTo(4);
    queue.take(jedis, service);
    assertThat(queue.size(jedis)).isEqualTo(3);
    queue.take(jedis, service);
    assertThat(queue.size(jedis)).isEqualTo(2);
    queue.take(jedis, service);
    assertThat(queue.size(jedis)).isEqualTo(1);
    queue.take(jedis, service);
    assertThat(queue.size(jedis)).isEqualTo(0);
    service.shutdown();
    assertThat(service.awaitTermination(0, SECONDS)).isTrue();
  }

  // Function under test: size
  // Reason for testing: size adjusts with offer and take
  // Failure explanation: size is incorrectly reporting the expected queue size
  @Test
  public void sizeAdjustPushPopPriority() throws Exception {
    // ARRANGE
    List<String> hashtags = RedisNodeHashes.getEvenlyDistributedHashes(jedis);
    BalancedRedisQueue queue =
        new BalancedRedisQueue("test", hashtags, RedisPriorityQueue::decorate);
    ExecutorService service = newSingleThreadExecutor();

    // ACT / ASSERT
    assertThat(queue.size(jedis)).isEqualTo(0);
    queue.offer(jedis, "foo1");
    assertThat(queue.size(jedis)).isEqualTo(1);
    queue.offer(jedis, "foo2");
    assertThat(queue.size(jedis)).isEqualTo(2);
    queue.offer(jedis, "foo3");
    assertThat(queue.size(jedis)).isEqualTo(3);
    queue.offer(jedis, "foo4");
    assertThat(queue.size(jedis)).isEqualTo(4);
    queue.offer(jedis, "foo5");
    assertThat(queue.size(jedis)).isEqualTo(5);
    queue.offer(jedis, "foo6");
    assertThat(queue.size(jedis)).isEqualTo(6);
    queue.take(jedis, service);
    assertThat(queue.size(jedis)).isEqualTo(5);
    queue.take(jedis, service);
    assertThat(queue.size(jedis)).isEqualTo(4);
    queue.take(jedis, service);
    assertThat(queue.size(jedis)).isEqualTo(3);
    queue.take(jedis, service);
    assertThat(queue.size(jedis)).isEqualTo(2);
    queue.take(jedis, service);
    assertThat(queue.size(jedis)).isEqualTo(1);
    queue.take(jedis, service);
    assertThat(queue.size(jedis)).isEqualTo(0);
    service.shutdown();
    assertThat(service.awaitTermination(0, SECONDS)).isTrue();
  }

  // Function under test: visit
  // Reason for testing: each element in the queue can be visited
  // Failure explanation: we are unable to visit each element in the queue
  @Test
  public void visitCheckVisitOfEachElement() throws Exception {
    // ARRANGE
    List<String> hashtags = RedisNodeHashes.getEvenlyDistributedHashes(jedis);
    BalancedRedisQueue queue = new BalancedRedisQueue("test", hashtags, RedisQueue::decorate);
    queue.offer(jedis, "element 1");
    queue.offer(jedis, "element 2");
    queue.offer(jedis, "element 3");
    queue.offer(jedis, "element 4");
    queue.offer(jedis, "element 5");
    queue.offer(jedis, "element 6");
    queue.offer(jedis, "element 7");
    queue.offer(jedis, "element 8");

    // ACT
    List<String> visited = new ArrayList<>();
    Visitor<BalancedQueueEntry> visitor =
        new Visitor<>() {
          public void visit(BalancedQueueEntry entry) {
            visited.add(entry.value());
          }
        };
    queue.visit(jedis, visitor);

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
  // Reason for testing: each element in the queue can be visited
  // Failure explanation: we are unable to visit each element in the queue
  @Test
  public void visitCheckVisitOfEachElementPriority() throws Exception {
    // ARRANGE
    List<String> hashtags = RedisNodeHashes.getEvenlyDistributedHashes(jedis);
    BalancedRedisQueue queue =
        new BalancedRedisQueue("test", hashtags, RedisPriorityQueue::decorate);
    queue.offer(jedis, "element 1");
    queue.offer(jedis, "element 2");
    queue.offer(jedis, "element 3");
    queue.offer(jedis, "element 4");
    queue.offer(jedis, "element 5");
    queue.offer(jedis, "element 6");
    queue.offer(jedis, "element 7");
    queue.offer(jedis, "element 8");

    // ACT
    List<String> visited = new ArrayList<>();
    Visitor<BalancedQueueEntry> visitor =
        new Visitor<>() {
          public void visit(BalancedQueueEntry entry) {
            visited.add(entry.value());
          }
        };
    queue.visit(jedis, visitor);

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

  // Function under test: isEvenlyDistributed
  // Reason for testing: an empty queue is always already evenly distributed
  // Failure explanation: evenly distributed is not working on the empty queue
  @Test
  public void isEvenlyDistributedEmptyIsEvenlyDistributed() throws Exception {
    // ARRANGE
    List<String> hashtags = RedisNodeHashes.getEvenlyDistributedHashes(jedis);
    BalancedRedisQueue queue = new BalancedRedisQueue("test", hashtags, RedisQueue::decorate);

    // ACT
    Boolean isEvenlyDistributed = queue.isEvenlyDistributed(jedis);

    // ASSERT
    assertThat(isEvenlyDistributed).isTrue();
  }

  // Function under test: isEvenlyDistributed
  // Reason for testing: an empty queue is always already evenly distributed
  // Failure explanation: evenly distributed is not working on the empty queue
  @Test
  public void isEvenlyDistributedEmptyIsEvenlyDistributedPriority() throws Exception {
    // ARRANGE
    List<String> hashtags = RedisNodeHashes.getEvenlyDistributedHashes(jedis);
    BalancedRedisQueue queue =
        new BalancedRedisQueue("test", hashtags, RedisPriorityQueue::decorate);

    // ACT
    Boolean isEvenlyDistributed = queue.isEvenlyDistributed(jedis);

    // ASSERT
    assertThat(isEvenlyDistributed).isTrue();
  }

  // Function under test: isEvenlyDistributed
  // Reason for testing: having 4 nodes and pushing 400 elements should show that the elements are
  // evenly distributed
  // Failure explanation: queue is not evenly distributing as it should
  @Test
  public void isEvenlyDistributedFourNodesFourHundredPushesIsEven() throws Exception {
    // ARRANGE
    List<String> hashtags = Arrays.asList("node1", "node2", "node3", "node4");
    BalancedRedisQueue queue = new BalancedRedisQueue("test", hashtags, RedisQueue::decorate);

    // ACT
    for (int i = 0; i < 400; ++i) {
      queue.offer(jedis, "foo");
    }
    Boolean isEvenlyDistributed = queue.isEvenlyDistributed(jedis);

    // ASSERT
    assertThat(isEvenlyDistributed).isTrue();
  }

  // Function under test: isEvenlyDistributed
  // Reason for testing: having 4 nodes and pushing 400 elements should show that the elements are
  // evenly distributed
  // Failure explanation: queue is not evenly distributing as it should
  @Test
  public void isEvenlyDistributedFourNodesFourHundredPushesIsEvenPriority() throws Exception {
    // ARRANGE
    List<String> hashtags = Arrays.asList("node1", "node2", "node3", "node4");
    BalancedRedisQueue queue =
        new BalancedRedisQueue("test", hashtags, RedisPriorityQueue::decorate);

    // ACT
    for (int i = 0; i < 400; ++i) {
      queue.offer(jedis, "foo");
    }
    Boolean isEvenlyDistributed = queue.isEvenlyDistributed(jedis);

    // ASSERT
    assertThat(isEvenlyDistributed).isTrue();
  }

  // Function under test: isEvenlyDistributed
  // Reason for testing: having 4 nodes and pushing 401 elements should show that the elements are
  // not evenly distributed
  // Failure explanation: queue is incorrectly reporting an even distribution
  @Test
  public void isEvenlyDistributedFourNodesFourHundredOnePushesIsNotEven() throws Exception {
    // ARRANGE
    List<String> hashtags = Arrays.asList("node1", "node2", "node3", "node4");
    BalancedRedisQueue queue = new BalancedRedisQueue("test", hashtags, RedisQueue::decorate);

    // ACT
    for (int i = 0; i < 401; ++i) {
      queue.offer(jedis, "foo" + i);
    }
    Boolean isEvenlyDistributed = queue.isEvenlyDistributed(jedis);

    // ASSERT
    assertThat(isEvenlyDistributed).isFalse();
  }

  // Function under test: isEvenlyDistributed
  // Reason for testing: having 4 nodes and pushing 401 elements should show that the elements are
  // not evenly distributed
  // Failure explanation: queue is incorrectly reporting an even distribution
  @Test
  public void isEvenlyDistributedFourNodesFourHundredOnePushesIsNotEvenPriority() throws Exception {
    // ARRANGE
    List<String> hashtags = Arrays.asList("node1", "node2", "node3", "node4");
    BalancedRedisQueue queue =
        new BalancedRedisQueue("test", hashtags, RedisPriorityQueue::decorate);

    // ACT
    for (int i = 0; i < 401; ++i) {
      queue.offer(jedis, "foo" + i);
    }
    Boolean isEvenlyDistributed = queue.isEvenlyDistributed(jedis);

    // ASSERT
    assertThat(isEvenlyDistributed).isFalse();
  }

  // Function under test: isEvenlyDistributed
  // Reason for testing: having a single node means the values are always evenly distributed over
  // that node
  // Failure explanation: queue is incorrectly reporting an even distribution
  @Test
  public void isEvenlyDistributedSingleNodeAlwaysEvenlyDistributes() throws Exception {
    // ARRANGE
    List<String> hashtags = Collections.singletonList("single_node");
    BalancedRedisQueue queue = new BalancedRedisQueue("test", hashtags, RedisQueue::decorate);
    ExecutorService service = newSingleThreadExecutor();

    // ACT / ASSERT
    queue.offer(jedis, "foo");
    assertThat(queue.isEvenlyDistributed(jedis)).isTrue();
    queue.offer(jedis, "foo");
    assertThat(queue.isEvenlyDistributed(jedis)).isTrue();
    queue.offer(jedis, "foo");
    assertThat(queue.isEvenlyDistributed(jedis)).isTrue();
    queue.offer(jedis, "foo");
    assertThat(queue.isEvenlyDistributed(jedis)).isTrue();
    queue.take(jedis, service);
    assertThat(queue.isEvenlyDistributed(jedis)).isTrue();
    queue.take(jedis, service);
    assertThat(queue.isEvenlyDistributed(jedis)).isTrue();
    queue.take(jedis, service);
    assertThat(queue.isEvenlyDistributed(jedis)).isTrue();
    queue.take(jedis, service);
    assertThat(queue.isEvenlyDistributed(jedis)).isTrue();
    service.shutdown();
    assertThat(service.awaitTermination(0, SECONDS)).isTrue();
  }

  // Function under test: isEvenlyDistributed
  // Reason for testing: having a single node means the values are always evenly distributed over
  // that node
  // Failure explanation: queue is incorrectly reporting an even distribution
  @Test
  public void isEvenlyDistributedSingleNodeAlwaysEvenlyDistributesPriority() throws Exception {
    // ARRANGE
    List<String> hashtags = Collections.singletonList("single_node");
    BalancedRedisQueue queue =
        new BalancedRedisQueue("test", hashtags, RedisPriorityQueue::decorate);
    ExecutorService service = newSingleThreadExecutor();

    // ACT / ASSERT
    queue.offer(jedis, "foo");
    assertThat(queue.isEvenlyDistributed(jedis)).isTrue();
    queue.offer(jedis, "bar");
    assertThat(queue.isEvenlyDistributed(jedis)).isTrue();
    queue.offer(jedis, "baz");
    assertThat(queue.isEvenlyDistributed(jedis)).isTrue();
    queue.offer(jedis, "quux");
    assertThat(queue.isEvenlyDistributed(jedis)).isTrue();
    queue.take(jedis, service);
    assertThat(queue.isEvenlyDistributed(jedis)).isTrue();
    queue.take(jedis, service);
    assertThat(queue.isEvenlyDistributed(jedis)).isTrue();
    queue.take(jedis, service);
    assertThat(queue.isEvenlyDistributed(jedis)).isTrue();
    queue.take(jedis, service);
    assertThat(queue.isEvenlyDistributed(jedis)).isTrue();
    service.shutdown();
    assertThat(service.awaitTermination(0, SECONDS)).isTrue();
  }

  // Function under test: isEvenlyDistributed
  // Reason for testing: this example shows how a two internal queues affect the even distribution
  // Failure explanation: queue is incorrectly reporting an even distribution
  @Test
  public void isEvenlyDistributedTwoNodeExample() throws Exception {
    // ARRANGE
    List<String> hashtags = Arrays.asList("node_1", "node_2");
    BalancedRedisQueue queue = new BalancedRedisQueue("test", hashtags, RedisQueue::decorate);
    ExecutorService service = newSingleThreadExecutor();

    // ACT / ASSERT
    assertThat(queue.isEvenlyDistributed(jedis)).isTrue();
    queue.offer(jedis, "foo1");
    assertThat(queue.isEvenlyDistributed(jedis)).isFalse();
    queue.offer(jedis, "foo2");
    assertThat(queue.isEvenlyDistributed(jedis)).isTrue();
    queue.offer(jedis, "foo3");
    assertThat(queue.isEvenlyDistributed(jedis)).isFalse();
    queue.offer(jedis, "foo4");
    assertThat(queue.isEvenlyDistributed(jedis)).isTrue();
    queue.offer(jedis, "foo5");
    assertThat(queue.isEvenlyDistributed(jedis)).isFalse();
    queue.offer(jedis, "foo6");
    assertThat(queue.isEvenlyDistributed(jedis)).isTrue();
    queue.offer(jedis, "foo7");
    assertThat(queue.isEvenlyDistributed(jedis)).isFalse();
    queue.take(jedis, service);
    assertThat(queue.isEvenlyDistributed(jedis)).isTrue();
    queue.take(jedis, service);
    assertThat(queue.isEvenlyDistributed(jedis)).isFalse();
    queue.take(jedis, service);
    assertThat(queue.isEvenlyDistributed(jedis)).isTrue();
    queue.take(jedis, service);
    assertThat(queue.isEvenlyDistributed(jedis)).isFalse();
    queue.take(jedis, service);
    assertThat(queue.isEvenlyDistributed(jedis)).isTrue();
    queue.take(jedis, service);
    assertThat(queue.isEvenlyDistributed(jedis)).isFalse();
    queue.take(jedis, service);
    assertThat(queue.isEvenlyDistributed(jedis)).isTrue();
    service.shutdown();
    assertThat(service.awaitTermination(0, SECONDS)).isTrue();
  }

  // Function under test: isEvenlyDistributed
  // Reason for testing: this example shows how a two internal queues affect the even distribution
  // Failure explanation: queue is incorrectly reporting an even distribution
  @Test
  public void isEvenlyDistributedTwoNodeExamplePriority() throws Exception {
    // ARRANGE
    List<String> hashtags = Arrays.asList("node_1", "node_2");
    BalancedRedisQueue queue =
        new BalancedRedisQueue("test", hashtags, RedisPriorityQueue::decorate);
    ExecutorService service = newSingleThreadExecutor();

    // ACT / ASSERT
    assertThat(queue.isEvenlyDistributed(jedis)).isTrue();
    queue.offer(jedis, "foo");
    assertThat(queue.isEvenlyDistributed(jedis)).isFalse();
    queue.offer(jedis, "foo1");
    assertThat(queue.isEvenlyDistributed(jedis)).isTrue();
    queue.offer(jedis, "foo2");
    assertThat(queue.isEvenlyDistributed(jedis)).isFalse();
    queue.offer(jedis, "foo3");
    assertThat(queue.isEvenlyDistributed(jedis)).isTrue();
    queue.offer(jedis, "foo4");
    assertThat(queue.isEvenlyDistributed(jedis)).isFalse();
    queue.offer(jedis, "foo5");
    assertThat(queue.isEvenlyDistributed(jedis)).isTrue();
    queue.offer(jedis, "foo6");
    assertThat(queue.isEvenlyDistributed(jedis)).isFalse();
    queue.take(jedis, service);
    assertThat(queue.isEvenlyDistributed(jedis)).isTrue();
    queue.take(jedis, service);
    assertThat(queue.isEvenlyDistributed(jedis)).isFalse();
    queue.take(jedis, service);
    assertThat(queue.isEvenlyDistributed(jedis)).isTrue();
    queue.take(jedis, service);
    assertThat(queue.isEvenlyDistributed(jedis)).isFalse();
    queue.take(jedis, service);
    assertThat(queue.isEvenlyDistributed(jedis)).isTrue();
    queue.take(jedis, service);
    assertThat(queue.isEvenlyDistributed(jedis)).isFalse();
    queue.take(jedis, service);
    assertThat(queue.isEvenlyDistributed(jedis)).isTrue();
    service.shutdown();
    assertThat(service.awaitTermination(0, SECONDS)).isTrue();
  }
}
