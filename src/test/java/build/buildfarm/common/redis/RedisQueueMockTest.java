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
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static redis.clients.jedis.args.ListDirection.LEFT;
import static redis.clients.jedis.args.ListDirection.RIGHT;

import build.buildfarm.common.StringVisitor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import redis.clients.jedis.Jedis;

/**
 * @class RedisQueueMockTest
 * @brief tests A redis queue.
 * @details A redis queue is an implementation of a queue data structure which internally uses redis
 *     to store and distribute the data. Its important to know that the lifetime of the queue
 *     persists before and after the queue data structure is created (since it exists in redis).
 *     Therefore, two redis queues with the same name, would in fact be the same underlying redis
 *     queue.
 */
@RunWith(JUnit4.class)
public class RedisQueueMockTest {
  @Mock private Jedis redis;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  // Function under test: redisQueue
  // Reason for testing: the queue can be constructed with a valid cluster instance and name
  // Failure explanation: the queue is throwing an exception upon construction
  @Test
  public void redisQueueConstructsWithoutError() throws Exception {
    // ACT
    new RedisQueue("test");
  }

  // Function under test: push
  // Reason for testing: the queue can have a value pushed onto it
  // Failure explanation: the queue is throwing an exception upon push
  @Test
  public void pushPushWithoutError() throws Exception {
    // ARRANGE
    RedisQueue queue = new RedisQueue("test");

    // ACT
    queue.push(redis, "foo");

    // ASSERT
    verify(redis, times(1)).lpush("test", "foo");
  }

  // Function under test: push
  // Reason for testing: the queue can have the different values pushed onto it
  // Failure explanation: the queue is throwing an exception upon pushing different values
  @Test
  public void pushPushDifferentWithoutError() throws Exception {
    // ARRANGE
    RedisQueue queue = new RedisQueue("test");

    // ACT
    queue.push(redis, "foo");
    queue.push(redis, "bar");

    // ASSERT
    verify(redis, times(1)).lpush("test", "foo");
    verify(redis, times(1)).lpush("test", "bar");
  }

  // Function under test: push
  // Reason for testing: the queue can have the same values pushed onto it
  // Failure explanation: the queue is throwing an exception upon pushing the same values
  @Test
  public void pushPushSameWithoutError() throws Exception {
    // ARRANGE
    RedisQueue queue = new RedisQueue("test");

    // ACT
    queue.push(redis, "foo");
    queue.push(redis, "foo");

    // ASSERT
    verify(redis, times(2)).lpush("test", "foo");
  }

  // Function under test: push
  // Reason for testing: the queue can have many values pushed into it
  // Failure explanation: the queue is throwing an exception upon pushing many values
  @Test
  public void pushPushMany() throws Exception {
    // ARRANGE
    RedisQueue queue = new RedisQueue("test");

    // ACT
    for (int i = 0; i < 1000; ++i) {
      queue.push(redis, "foo" + i);
    }

    // ASSERT
    for (int i = 0; i < 1000; ++i) {
      verify(redis, times(1)).lpush("test", "foo" + i);
    }
  }

  // Function under test: push
  // Reason for testing: the queue size increases as elements are pushed
  // Failure explanation: the queue size is not accurately reflecting the pushes
  @Test
  public void pushCallsLPush() throws Exception {
    // ARRANGE
    RedisQueue queue = new RedisQueue("test");

    // ACT
    queue.push(redis, "foo");
    queue.push(redis, "foo");
    queue.push(redis, "foo");
    queue.push(redis, "foo");
    queue.push(redis, "foo");
    queue.push(redis, "foo");
    queue.push(redis, "foo");
    queue.push(redis, "foo");
    queue.push(redis, "foo");
    queue.push(redis, "foo");

    // ASSERT
    verify(redis, times(10)).lpush("test", "foo");
  }

  // Function under test: removeFromDequeue
  // Reason for testing: we can remove an element from the dequeue
  // Failure explanation: we are either unable to get an element into the dequeue or unable to
  // remove it
  @Test
  public void removeFromDequeueRemoveADequeueValue() throws Exception {
    // ARRANGE
    when(redis.lrem("test_dequeue", -1, "foo")).thenReturn(1L);
    RedisQueue queue = new RedisQueue("test");

    // ACT
    boolean wasRemoved = queue.removeFromDequeue(redis, "foo");

    // ASSERT
    assertThat(wasRemoved).isTrue();
    verify(redis, times(1)).lrem("test_dequeue", -1, "foo");
  }

  // Function under test: dequeue
  // Reason for testing: the element is able to be dequeued
  // Failure explanation: something prevented the element from being dequeued
  @Test
  public void dequeueElementCanBeDequeuedWithTimeout() throws Exception {
    // ARRANGE
    when(redis.blmove(eq("test"), eq("test_dequeue"), eq(RIGHT), eq(LEFT), any(double.class)))
        .thenReturn("foo");
    RedisQueue queue = new RedisQueue("test");
    ExecutorService service = newSingleThreadExecutor();

    // ACT
    String val = queue.dequeue(redis, 1, service);
    service.shutdown();

    // ASSERT
    assertThat(val).isEqualTo("foo");
    assertThat(service.awaitTermination(1, SECONDS)).isTrue();
  }

  // Function under test: dequeue
  // Reason for testing: element is not dequeued
  // Failure explanation: element was dequeued
  @Test
  public void dequeueElementIsNotDequeuedIfTimeRunsOut() throws Exception {
    // ARRANGE
    when(redis.blmove(eq("test"), eq("test_dequeue"), eq(RIGHT), eq(LEFT), any(double.class)))
        .thenReturn(null);
    RedisQueue queue = new RedisQueue("test");
    ExecutorService service = newSingleThreadExecutor();

    // ACT
    String val = queue.dequeue(redis, 5, service);
    service.shutdown();

    // ASSERT
    // future submission may still be completing after interrupt
    assertThat(service.awaitTermination(1, SECONDS)).isTrue();
    assertThat(val).isEqualTo(null);
  }

  // Function under test: dequeue
  // Reason for testing: the dequeue is interrupted
  // Failure explanation: the dequeue was not interrupted as expected
  @Test
  public void dequeueInterrupt() throws Exception {
    // ARRANGE
    when(redis.blmove(eq("test"), eq("test_dequeue"), eq(RIGHT), eq(LEFT), any(double.class)))
        .thenReturn(null);
    RedisQueue queue = new RedisQueue("test");
    ExecutorService service = newSingleThreadExecutor();

    // ACT
    Thread call =
        new Thread(
            () -> {
              try {
                queue.dequeue(redis, 100000, service);
              } catch (Exception e) {
              }
            });
    call.start();
    call.interrupt();
    call.join();
  }

  // Function under test: nonBlockingDequeue
  // Reason for testing: the element is able to be dequeued
  // Failure explanation: something prevented the element from being dequeued
  @Test
  public void nonBlockingDequeueElementCanBeDequeued() throws Exception {
    // ARRANGE
    when(redis.lmove("test", "test_dequeue", RIGHT, LEFT)).thenReturn("foo");
    RedisQueue queue = new RedisQueue("test");

    // ACT
    String val = queue.nonBlockingDequeue(redis);

    // ASSERT
    assertThat(val).isEqualTo("foo");
  }

  // Function under test: getName
  // Reason for testing: the name can be received
  // Failure explanation: name does not match what it should
  @Test
  public void getNameNameIsStored() throws Exception {
    // ARRANGE
    RedisQueue queue = new RedisQueue("queue_name");

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
    RedisQueue queue = new RedisQueue("queue_name");

    // ACT
    String name = queue.getDequeueName();

    // ASSERT
    assertThat(name).isEqualTo("queue_name_dequeue");
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
    RedisQueue queue = new RedisQueue("test");

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
    RedisQueue queue = new RedisQueue("test");

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
}
