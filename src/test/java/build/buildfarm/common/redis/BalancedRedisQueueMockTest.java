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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

import build.buildfarm.common.StringVisitor;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import redis.clients.jedis.JedisCluster;

/**
 * @class BalancedRedisQueueMockTest
 * @brief tests A balanced redis queue.
 * @details A balanced redis queue is an implementation of a queue data structure which internally
 *     uses multiple redis nodes to distribute the data across the cluster. Its important to know
 *     that the lifetime of the queue persists before and after the queue data structure is created
 *     (since it exists in redis). Therefore, two redis queues with the same name, would in fact be
 *     the same underlying redis queues.
 */
@RunWith(JUnit4.class)
public class BalancedRedisQueueMockTest {

  @Mock private JedisCluster redis;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  // Function under test: removeFromDequeue
  // Reason for testing: removing returns false because the queue is empty and there is nothing to
  // remove
  // Failure explanation: the queue was either not empty, or an error occured while removing from an
  // empty queue
  @Test
  public void removeFromDequeueFalseOnEmpty() throws Exception {

    // ARRANGE
    when(redis.lrem(any(String.class), any(Long.class), any(String.class))).thenReturn(0L);
    BalancedRedisQueue queue = new BalancedRedisQueue("test", ImmutableList.of());

    // ACT
    Boolean success = queue.removeFromDequeue(redis, "foo");

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
    when(redis.lrem(any(String.class), any(Long.class), any(String.class))).thenReturn(0L);
    BalancedRedisQueue queue = new BalancedRedisQueue("test", ImmutableList.of());

    // ACT
    Boolean success = queue.removeFromDequeue(redis, "baz");

    // ASSERT
    assertThat(success).isFalse();
  }

  // Function under test: removeFromDequeue
  // Reason for testing: removing returns true because the queue contained the value before removing
  // Failure explanation: the queue either did not contain the value or incorrectly reported a
  // deletion
  @Test
  public void removeFromDequeueTrueWhenValueExists() throws Exception {

    // ARRANGE
    when(redis.lrem(any(String.class), any(Long.class), any(String.class))).thenReturn(1L);
    BalancedRedisQueue queue = new BalancedRedisQueue("test", ImmutableList.of());

    // ACT
    Boolean success = queue.removeFromDequeue(redis, "bar");

    // ASSERT
    assertThat(success).isTrue();
  }

  // Function under test: dequeue
  // Reason for testing: the element is dequeued via nonblocking
  // Failure explanation: the element failed to dequeue
  @Test
  public void dequeueExponentialBackoffElementDequeuedOnNonBlock() throws Exception {

    // MOCK
    when(redis.rpoplpush(any(String.class), any(String.class))).thenReturn("foo");

    // ARRANGE
    BalancedRedisQueue queue = new BalancedRedisQueue("test", ImmutableList.of());

    // ACT
    String val = queue.dequeue(redis);

    // ASSERT
    assertThat(val).isEqualTo("foo");
  }

  // Function under test: dequeue
  // Reason for testing: the element is dequeued via nonblocking
  // Failure explanation: the element failed to dequeue
  @Test
  public void dequeueExponentialBackoffElementDequeuedOnBlock() throws Exception {

    // MOCK
    when(redis.rpoplpush(any(String.class), any(String.class))).thenReturn(null);
    when(redis.brpoplpush(any(String.class), any(String.class), any(int.class))).thenReturn("foo");

    // ARRANGE
    BalancedRedisQueue queue = new BalancedRedisQueue("test", ImmutableList.of());

    // ACT
    String val = queue.dequeue(redis);

    // ASSERT
    assertThat(val).isEqualTo("foo");
  }

  // Function under test: getCurrentPopQueue
  // Reason for testing: the current pop queue can be retrieved
  // Failure explanation: it was a failure to get the current pop queue
  @Test
  public void getCurrentPopQueueCanGet() throws Exception {

    // ARRANGE
    BalancedRedisQueue queue = new BalancedRedisQueue("queue_name", ImmutableList.of());

    // ACT
    RedisQueue internalQueue = queue.getCurrentPopQueue();
  }

  // Function under test: getCurrentPopQueueIndex
  // Reason for testing: the current pop queue index can be retrieved
  // Failure explanation: it was a failure to get the current pop queue index
  @Test
  public void getCurrentPopQueueIndexCanGet() throws Exception {

    // ARRANGE
    BalancedRedisQueue queue = new BalancedRedisQueue("queue_name", ImmutableList.of());

    // ACT
    int index = queue.getCurrentPopQueueIndex();
  }

  // Function under test: getInternalQueue
  // Reason for testing: a queue can be retrieved by an index
  // Failure explanation: the queue could not be retrieved by an index
  @Test
  public void getInternalQueueCanGet() throws Exception {

    // ARRANGE
    BalancedRedisQueue queue = new BalancedRedisQueue("queue_name", ImmutableList.of());

    // ACT
    RedisQueue internalQueue = queue.getInternalQueue(0);
  }

  // Function under test: getDequeueName
  // Reason for testing: the dequeue name is as expected
  // Failure explanation: the dequeue name is not as expected
  @Test
  public void getDequeueNameCanGet() throws Exception {

    // ARRANGE
    BalancedRedisQueue queue = new BalancedRedisQueue("queue_name", ImmutableList.of());

    // ACT
    String name = queue.getDequeueName();

    // ASSERT
    assertThat(name).isEqualTo("queue_name_dequeue");
  }

  // Function under test: getName
  // Reason for testing: the name can be received
  // Failure explanation: name does not match what it should
  @Test
  public void getNameNameIsStored() throws Exception {

    // ARRANGE
    BalancedRedisQueue queue = new BalancedRedisQueue("queue_name", ImmutableList.of());

    // ACT
    String name = queue.getName();

    // ASSERT
    assertThat(name).isEqualTo("queue_name");
  }

  // Function under test: size
  // Reason for testing: the shared initial size is 0
  // Failure explanation: size is incorrectly reporting the expected queue size
  @Test
  public void sizeInitialSizeIsZero() throws Exception {

    // MOCK
    when(redis.llen(any(String.class))).thenReturn(0L);

    // ARRANGE
    BalancedRedisQueue queue = new BalancedRedisQueue("test", ImmutableList.of());

    // ACT
    long size = queue.size(redis);

    // ASSERT
    assertThat(size).isEqualTo(0);
  }

  // Function under test: visit
  // Reason for testing: each element in the queue can be visited
  // Failure explanation: we are unable to visit each element in the queue
  @Test
  public void visitCheckVisitOfEachElement() throws Exception {

    // MOCK
    when(redis.lrange(any(String.class), any(Long.class), any(Long.class)))
        .thenReturn(
            Arrays.asList(
                "element 1",
                "element 2",
                "element 3",
                "element 4",
                "element 5",
                "element 6",
                "element 7",
                "element 8"));

    // ARRANGE
    BalancedRedisQueue queue = new BalancedRedisQueue("test", ImmutableList.of());
    queue.push(redis, "element 1");
    queue.push(redis, "element 2");
    queue.push(redis, "element 3");
    queue.push(redis, "element 4");
    queue.push(redis, "element 5");
    queue.push(redis, "element 6");
    queue.push(redis, "element 7");
    queue.push(redis, "element 8");

    // ACT
    List<String> visited = new ArrayList<String>();
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

  // Function under test: visitDequeue
  // Reason for testing: each element in the queue can be visited
  // Failure explanation: we are unable to visit each element in the queue
  @Test
  public void visitDequeueCheckVisitOfEachElement() throws Exception {

    // MOCK
    when(redis.lrange(any(String.class), any(Long.class), any(Long.class)))
        .thenReturn(
            Arrays.asList(
                "element 1",
                "element 2",
                "element 3",
                "element 4",
                "element 5",
                "element 6",
                "element 7",
                "element 8"));

    // ARRANGE
    BalancedRedisQueue queue = new BalancedRedisQueue("test", ImmutableList.of());

    // ACT
    List<String> visited = new ArrayList<String>();
    StringVisitor visitor =
        new StringVisitor() {
          public void visit(String entry) {
            visited.add(entry);
          }
        };
    queue.visitDequeue(redis, visitor);

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

    // MOCK
    when(redis.llen(any(String.class))).thenReturn(0L);

    // ARRANGE
    BalancedRedisQueue queue = new BalancedRedisQueue("test", ImmutableList.of());

    // ACT
    Boolean isEvenlyDistributed = queue.isEvenlyDistributed(redis);

    // ASSERT
    assertThat(isEvenlyDistributed).isTrue();
  }
}
