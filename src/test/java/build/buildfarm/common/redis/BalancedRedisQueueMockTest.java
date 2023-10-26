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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import build.buildfarm.common.StringVisitor;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import redis.clients.jedis.Connection;
import redis.clients.jedis.Jedis;
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
  @Mock private Connection connection;
  @Mock QueueInterface subQueue;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(redis.getConnectionFromSlot(any(Integer.class))).thenReturn(connection);
    when(subQueue.getName()).thenReturn("test");
  }

  // Function under test: removeFromDequeue
  // Reason for testing: removing returns false because the queue does not contain the value to be
  // removed
  // Failure explanation: the queue was either contained the value or incorrectly reported a
  // deletion
  @Test
  public void removeFromDequeueFalseWhenValueIsMissing() throws Exception {
    // ARRANGE
    when(subQueue.removeFromDequeue(any(Jedis.class), any(String.class))).thenReturn(false);
    BalancedRedisQueue queue = new BalancedRedisQueue("test", -1, ImmutableList.of(subQueue));

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
    when(subQueue.removeFromDequeue(any(Jedis.class), any(String.class))).thenReturn(true);
    BalancedRedisQueue queue = new BalancedRedisQueue("test", -1, ImmutableList.of(subQueue));

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
    when(subQueue.nonBlockingDequeue(any(Jedis.class))).thenReturn("foo");
    ExecutorService service = mock(ExecutorService.class);

    // ARRANGE
    BalancedRedisQueue queue = new BalancedRedisQueue("test", -1, ImmutableList.of(subQueue));

    // ACT
    String val = queue.dequeue(redis, service);

    // ASSERT
    verifyNoInteractions(service);
    assertThat(val).isEqualTo("foo");
  }

  // Function under test: dequeue
  // Reason for testing: the element is dequeued via nonblocking
  // Failure explanation: the element failed to dequeue
  @Test
  public void dequeueExponentialBackoffElementDequeuedOnBlock() throws Exception {
    // MOCK
    when(subQueue.nonBlockingDequeue(any(Jedis.class))).thenReturn(null);
    when(subQueue.dequeue(any(Jedis.class), any(Integer.class), any(ExecutorService.class)))
        .thenReturn("foo");

    // ARRANGE
    BalancedRedisQueue queue = new BalancedRedisQueue("test", -1, ImmutableList.of(subQueue));
    ExecutorService service = mock(ExecutorService.class);

    // ACT
    String val = queue.dequeue(redis, service);

    // ASSERT
    verifyNoInteractions(service);
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
    queue.getCurrentPopQueue();
  }

  // Function under test: getCurrentPopQueueIndex
  // Reason for testing: the current pop queue index can be retrieved
  // Failure explanation: it was a failure to get the current pop queue index
  @Test
  public void getCurrentPopQueueIndexCanGet() throws Exception {
    // ARRANGE
    BalancedRedisQueue queue = new BalancedRedisQueue("queue_name", ImmutableList.of());

    // ACT
    queue.getCurrentPopQueueIndex();
  }

  // Function under test: getInternalQueue
  // Reason for testing: a queue can be retrieved by an index
  // Failure explanation: the queue could not be retrieved by an index
  @Test
  public void getInternalQueueCanGet() throws Exception {
    // ARRANGE
    BalancedRedisQueue queue = new BalancedRedisQueue("queue_name", ImmutableList.of());

    // ACT
    queue.getInternalQueue(0);
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
    when(subQueue.size(any(Jedis.class))).thenReturn(0L);

    // ARRANGE
    BalancedRedisQueue queue = new BalancedRedisQueue("test", -1, ImmutableList.of(subQueue));

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
    doAnswer(
            invocation -> {
              StringVisitor visitor = invocation.getArgument(1);
              for (int i = 1; i <= 8; i++) {
                visitor.visit("element " + i);
              }
              return null;
            })
        .when(subQueue)
        .visit(any(Jedis.class), any(StringVisitor.class));

    // ARRANGE
    BalancedRedisQueue queue = new BalancedRedisQueue("test", -1, ImmutableList.of(subQueue));

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
    assertThat(visited)
        .containsExactly(
            "element 1",
            "element 2",
            "element 3",
            "element 4",
            "element 5",
            "element 6",
            "element 7",
            "element 8");
  }

  // Function under test: visitDequeue
  // Reason for testing: each element in the queue can be visited
  // Failure explanation: we are unable to visit each element in the queue
  @Test
  public void visitDequeueCheckVisitOfEachElement() throws Exception {
    // MOCK
    doAnswer(
            invocation -> {
              StringVisitor visitor = invocation.getArgument(1);
              for (int i = 1; i <= 8; i++) {
                visitor.visit("element " + i);
              }
              return null;
            })
        .when(subQueue)
        .visitDequeue(any(Jedis.class), any(StringVisitor.class));

    // ARRANGE
    BalancedRedisQueue queue = new BalancedRedisQueue("test", -1, ImmutableList.of(subQueue));

    // ACT
    List<String> visited = new ArrayList<>();
    StringVisitor visitor =
        new StringVisitor() {
          public void visit(String entry) {
            visited.add(entry);
          }
        };
    queue.visitDequeue(redis, visitor);

    // ASSERT
    assertThat(visited)
        .containsExactly(
            "element 1",
            "element 2",
            "element 3",
            "element 4",
            "element 5",
            "element 6",
            "element 7",
            "element 8");
  }

  // Function under test: isEvenlyDistributed
  // Reason for testing: an empty queue is always already evenly distributed
  // Failure explanation: evenly distributed is not working on the empty queue
  @Test
  public void emptyIsEvenlyDistributed() throws Exception {
    // MOCK
    when(subQueue.size(any(Jedis.class))).thenReturn(0L);

    // ARRANGE
    BalancedRedisQueue queue = new BalancedRedisQueue("test", -1, ImmutableList.of(subQueue));

    // ACT
    Boolean isEvenlyDistributed = queue.isEvenlyDistributed(redis);

    // ASSERT
    verify(subQueue, times(1)).size(any(Jedis.class));
    assertThat(isEvenlyDistributed).isTrue();
  }

  // Function under test: canQueue
  // Reason for testing: infinite queues allow queuing
  // Failure explanation: the queue is not accepting queuing when it should
  @Test
  public void canQueueInfiniteQueueAllowsQueuing() throws Exception {
    // ARRANGE
    BalancedRedisQueue queue = new BalancedRedisQueue("test", -1, ImmutableList.of(subQueue));

    // ACT
    boolean canQueue = queue.canQueue(redis);

    // ASSERT
    verifyNoInteractions(subQueue);
    assertThat(canQueue).isTrue();
  }

  // Function under test: canQueue for priority
  // Reason for testing: Full queues do not allow queuing
  // Failure explanation: the queue is still allows queueing despite being full
  @Test
  public void canQueueFullQueueNotAllowsQueueing() throws Exception {
    // MOCK
    when(subQueue.size(any(Jedis.class))).thenReturn(123L);

    // ARRANGE
    BalancedRedisQueue queue = new BalancedRedisQueue("test", 123, ImmutableList.of(subQueue));

    // ACT
    boolean canQueue = queue.canQueue(redis);

    // ASSERT
    verify(subQueue, times(1)).size(any(Jedis.class));
    assertThat(canQueue).isFalse();
  }
}
