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

import static com.google.common.collect.Iterables.partition;
import static com.google.common.collect.Lists.newArrayList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static redis.clients.jedis.args.ListDirection.LEFT;
import static redis.clients.jedis.args.ListDirection.RIGHT;

import build.buildfarm.common.StringVisitor;
import com.google.common.collect.ImmutableList;
import java.time.Duration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import redis.clients.jedis.Jedis;

@RunWith(JUnit4.class)
public class RedisQueueMockTest {
  @Mock private Jedis redis;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void decorateSucceeds() {
    RedisQueue.decorate(redis, "test", RedisQueue.UNLIMITED_QUEUE_DEPTH);
  }

  @Test
  public void offerShouldLPush() {
    RedisQueue queue = new RedisQueue(redis, "test");

    queue.offer("foo");

    verify(redis, times(1)).lpush("test", "foo");
  }

  @Test
  public void removeFromDequeueShouldLRemTail() {
    RedisQueue queue = new RedisQueue(redis, "test");

    queue.removeFromDequeue("foo");

    verify(redis, times(1)).lrem(queue.getDequeueName(), -1, "foo");
  }

  @Test
  public void removeAllShouldLRemAll() {
    RedisQueue queue = new RedisQueue(redis, "test");

    queue.removeAll("foo");

    verify(redis, times(1)).lrem("test", 0, "foo");
  }

  @Test
  public void takeShouldBLMove() {
    RedisQueue queue = new RedisQueue(redis, "test");

    queue.take(Duration.ofMillis(1470));

    verify(redis, times(1)).blmove("test", queue.getDequeueName(), RIGHT, LEFT, 1.47);
  }

  @Test
  public void pollShouldLMove() {
    RedisQueue queue = new RedisQueue(redis, "test");

    queue.poll();

    verify(redis, times(1)).lmove("test", queue.getDequeueName(), RIGHT, LEFT);
  }

  @Test
  public void sizeShouldLLen() {
    RedisQueue queue = new RedisQueue(redis, "test");

    queue.size();

    verify(redis, times(1)).llen("test");
  }

  private void arrangeVisitLRange(String name, int listPageSize, Iterable<String> entries) {
    int index = 0;
    int nextIndex = listPageSize;
    for (Iterable<String> page : partition(entries, listPageSize)) {
      when(redis.lrange(name, index, nextIndex - 1)).thenReturn(newArrayList(page));
      index = nextIndex;
      nextIndex += listPageSize;
    }
  }

  private void verifyVisitLRange(
      String name, StringVisitor visitor, int listPageSize, Iterable<String> entries) {
    int pageCount = listPageSize;
    int index = 0;
    int nextIndex = listPageSize;
    for (String entry : entries) {
      verify(visitor, times(1)).visit(entry);
      if (--pageCount == 0) {
        verify(redis, times(1)).lrange(name, index, nextIndex - 1);
        index = nextIndex;
        nextIndex += listPageSize;
        pageCount = listPageSize;
      }
    }
    if (pageCount != 0) {
      verify(redis, times(1)).lrange(name, index, nextIndex - 1);
    }
  }

  private final Iterable<String> VISIT_ENTRIES = ImmutableList.of("one", "two", "three", "four");

  @Test
  public void visitShouldLRange() {
    int listPageSize = 3;
    RedisQueue queue =
        new RedisQueue(redis, "test", RedisQueue.UNLIMITED_QUEUE_DEPTH, listPageSize);
    arrangeVisitLRange("test", listPageSize, VISIT_ENTRIES);
    StringVisitor visitor = mock(StringVisitor.class);

    queue.visit(visitor);

    verifyVisitLRange("test", visitor, listPageSize, VISIT_ENTRIES);
  }

  @Test
  public void visitDequeueShouldLRange() {
    int listPageSize = 3;
    RedisQueue queue =
        new RedisQueue(redis, "test", RedisQueue.UNLIMITED_QUEUE_DEPTH, listPageSize);
    arrangeVisitLRange(queue.getDequeueName(), listPageSize, VISIT_ENTRIES);
    StringVisitor visitor = mock(StringVisitor.class);

    queue.visitDequeue(visitor);

    verifyVisitLRange(queue.getDequeueName(), visitor, listPageSize, VISIT_ENTRIES);
  }
}
