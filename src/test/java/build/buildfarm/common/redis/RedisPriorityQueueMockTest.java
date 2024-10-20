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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import build.buildfarm.common.StringVisitor;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import redis.clients.jedis.Jedis;

/**
 * @class RedisPriorityQueueMockTest
 * @brief tests A redis queue.
 * @details A redis queue is an implementation of a queue data structure which internally uses redis
 *     to store and distribute the data. Its important to know that the lifetime of the queue
 *     persists before and after the queue data structure is created (since it exists in redis).
 *     Therefore, two redis queues with the same name, would in fact be the same underlying redis
 *     queue.
 */
@RunWith(JUnit4.class)
public class RedisPriorityQueueMockTest {
  @Mock private Jedis redis;
  @Mock private Clock clock;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  // Function under test: RedisPriorityQueue
  // Reason for testing: the queue can be constructed with a valid cluster instance and name
  // Failure explanation: the queue is throwing an exception upon construction
  @Test
  public void redisPriorityQueueConstructsWithoutError() throws Exception {
    // ACT
    new RedisPriorityQueue(redis, "test");
  }

  // Function under test: offer
  // Reason for testing: the queue can have a value offered to it
  // Failure explanation: the queue is throwing an exception upon offer
  @Test
  public void offerOfferWithoutError() throws Exception {
    // ARRANGE
    when(clock.millis()).thenReturn(123L);
    RedisPriorityQueue queue = new RedisPriorityQueue(redis, "test", clock);

    // ACT
    queue.offer("foo");

    // ASSERT
    verify(redis, times(1)).zadd("test", 0, "123:foo");
  }

  // Function under test: offer
  // Reason for testing: the queue can have the different values offered onto it
  // Failure explanation: the queue is throwing an exception upon offering different values
  @Test
  public void offerOfferDifferentWithoutError() throws Exception {
    // ARRANGE
    when(clock.millis()).thenReturn(123L, 124L);
    RedisPriorityQueue queue = new RedisPriorityQueue(redis, "test", clock);

    // ACT
    queue.offer("foo");
    queue.offer("bar");

    // ASSERT
    verify(redis, times(1)).zadd("test", 0, "123:foo");
    verify(redis, times(1)).zadd("test", 0, "124:bar");
  }

  // Function under test: offer
  // Reason for testing: the queue can have the same values offered to it
  // Failure explanation: the queue is throwing an exception upon offering the same values
  @Test
  public void offerOfferSameWithoutError() throws Exception {
    // ARRANGE
    when(clock.millis()).thenReturn(123L, 124L);
    RedisPriorityQueue queue = new RedisPriorityQueue(redis, "test", clock);

    // ACT
    queue.offer("foo");
    queue.offer("foo");

    // ASSERT
    verify(redis, times(1)).zadd("test", 0, "123:foo");
    verify(redis, times(1)).zadd("test", 0, "124:foo");
  }

  // Function under test: offer
  // Reason for testing: the queue can have the same values offered to it
  // Failure explanation: the queue throws an exception when offered the same values
  @Test
  public void offerOfferPriorityWithoutError() throws Exception {
    // ARRANGE
    when(clock.millis()).thenReturn(123L, 124L);
    RedisPriorityQueue queue = new RedisPriorityQueue(redis, "test", clock);

    // ACT
    queue.offer("foo", 1);
    queue.offer("foo2", 2);

    // ASSERT
    verify(redis, times(1)).zadd("test", 1, "123:foo");
    verify(redis, times(1)).zadd("test", 2, "124:foo2");
  }

  // Function under test: offer
  // Reason for testing: the queue can have many values offered to it
  // Failure explanation: the queue throws an exception when offered many values
  @Test
  public void offerMany() throws Exception {
    // ARRANGE
    when(clock.millis()).thenReturn(123L);
    RedisPriorityQueue queue = new RedisPriorityQueue(redis, "test", clock);

    // ACT
    for (int i = 0; i < 1000; ++i) {
      queue.offer("foo" + i);
    }

    // ASSERT
    verify(redis, times(1000)).zadd(eq("test"), eq(0.0), any(String.class));
  }

  // Function under test: offer
  // Reason for testing: the queue size increases as elements are offered
  // Failure explanation: the queue size does not reflect the offerings
  @Test
  public void offerCallsZAdd() throws Exception {
    // ARRANGE
    when(clock.millis()).thenReturn(123L, 124L, 125L);
    RedisPriorityQueue queue = new RedisPriorityQueue(redis, "test", clock);

    // ACT
    queue.offer("foo", 0);
    queue.offer("foo1", 2);
    queue.offer("foo2", 2);

    // ASSERT
    verify(clock, times(3)).millis();
    verify(redis, times(1)).zadd("test", 0, "123:foo");
    verify(redis, times(1)).zadd("test", 2, "124:foo1");
    verify(redis, times(1)).zadd("test", 2, "125:foo2");
  }

  // Function under test: removeFromDequeue
  // Reason for testing: we can remove an element from the dequeue
  // Failure explanation: we are either unable to get an element into the dequeue or unable to
  // remove it
  @Test
  public void removeFromDequeueRemoveADequeueValue() throws Exception {
    // ARRANGE
    when(redis.lrem("test_dequeue", -1, "foo")).thenReturn(1L);
    RedisPriorityQueue queue = new RedisPriorityQueue(redis, "test");

    // ACT
    boolean wasRemoved = queue.removeFromDequeue("foo");

    // ASSERT
    assertThat(wasRemoved).isTrue();
    verify(redis, times(1)).lrem("test_dequeue", -1, "foo");
  }

  // Function under test: take
  // Reason for testing: the element is able to be taken
  // Failure explanation: something prevented the element from being taken
  @Test
  public void dequeueElementCanBeDequeuedWithTimeout() throws Exception {
    // ARRANGE
    when(redis.eval(any(String.class), any(List.class), any(List.class))).thenReturn("foo");
    RedisPriorityQueue queue = new RedisPriorityQueue(redis, "test");

    // ACT
    String val = queue.take(Duration.ofSeconds(1));

    // ASSERT
    assertThat(val).isEqualTo("foo");
  }

  // Function under test: take
  // Reason for testing: element is not taken
  // Failure explanation: element was taken
  @Test
  public void dequeueElementIsNotDequeuedIfTimeRunsOut() throws Exception {
    // ARRANGE
    when(redis.eval(any(String.class), any(List.class), any(List.class))).thenReturn(null);
    RedisPriorityQueue queue = new RedisPriorityQueue(redis, "test");

    // ACT
    String val = queue.take(Duration.ofMillis(100));

    // ASSERT
    assertThat(val).isEqualTo(null);
  }

  // Function under test: take
  // Reason for testing: the take is interrupted
  // Failure explanation: the take was not interrupted as expected
  @Test
  public void dequeueInterrupt() throws Exception {
    // ARRANGE
    when(redis.eval(any(String.class), any(List.class), any(List.class))).thenReturn(null);
    RedisPriorityQueue queue = new RedisPriorityQueue(redis, "test");

    // ACT
    Thread call =
        new Thread(
            () -> {
              try {
                queue.take(Duration.ofDays(1));
              } catch (Exception e) {
              }
            });
    call.start();
    call.interrupt();
    call.join();
  }

  // Function under test: poll
  // Reason for testing: the element is able to be polled
  // Failure explanation: something prevented the element from being polled
  @Test
  public void nonBlockingDequeueElementCanBeDequeued() throws Exception {
    // ARRANGE
    when(redis.eval(any(String.class), any(List.class), any(List.class))).thenReturn("foo");
    RedisPriorityQueue queue = new RedisPriorityQueue(redis, "test");

    // ACT
    String val = queue.poll();

    // ASSERT
    assertThat(val).isEqualTo("foo");
  }

  // Function under test: visit
  // Reason for testing: each element in the queue can be visited
  // Failure explanation: we are unable to visit each element in the queue
  @Test
  public void visitCheckVisitOfEachElement() throws Exception {
    // MOCK
    when(redis.zrange(any(String.class), any(Long.class), any(Long.class)))
        .thenReturn(
            Stream.of(
                    "element 1",
                    "element 2",
                    "element 3",
                    "element 4",
                    "element 5",
                    "element 6",
                    "element 7",
                    "element 8")
                .collect(Collectors.toList()));

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
    RedisPriorityQueue queue = new RedisPriorityQueue(redis, "test");

    // ACT
    List<String> visited = new ArrayList<>();
    StringVisitor visitor =
        new StringVisitor() {
          public void visit(String entry) {
            visited.add(entry);
          }
        };
    queue.visitDequeue(visitor);

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
