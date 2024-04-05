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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import build.buildfarm.common.StringVisitor;
import build.buildfarm.common.config.BuildfarmConfigs;
import build.buildfarm.instance.shard.JedisClusterFactory;
import com.google.common.collect.ImmutableList;
import java.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.UnifiedJedis;

/**
 * @class RedisQueueTest
 * @brief tests A redis queue.
 * @details A redis queue is an implementation of a queue data structure which internally uses redis
 *     to store and distribute the data. Its important to know that the lifetime of the queue
 *     persists before and after the queue data structure is created (since it exists in redis).
 *     Therefore, two redis queues with the same name, would in fact be the same underlying redis
 *     queue.
 */
@RunWith(JUnit4.class)
public class RedisQueueTest {
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
    redis.flushDB();
  }

  @After
  public void tearDown() {
    redis.flushDB();
    redis.close();
    pooled.close();
  }

  @Test
  public void decorateSucceeds() {
    RedisQueue.decorate(redis, "test", RedisQueue.UNLIMITED_QUEUE_DEPTH);
  }

  @Test
  public void unlimitedQueueOfferShouldContain() {
    RedisQueue queue = new RedisQueue(redis, "test");

    assertThat(queue.offer("foo"));

    assertThat(redis.lrange("test", 0, -1)).containsExactly("foo");
  }

  @Test
  public void limitedQueueOfferShouldContain() {
    RedisQueue queue = new RedisQueue(redis, "test", 1);

    assertThat(queue.offer("foo"));

    assertThat(redis.lrange("test", 0, -1)).containsExactly("foo");
  }

  @Test
  public void limitedQueueFailsOnFull() {
    RedisQueue queue = new RedisQueue(redis, "test", 0);

    assertThat(!queue.offer("foo"));

    assertThat(redis.lrange("test", 0, -1)).containsExactly();
  }

  @Test
  public void removeFromDequeueShouldExclude() {
    RedisQueue queue = new RedisQueue(redis, "test");
    redis.lpush(queue.getDequeueName(), "foo", "bar", "foo");

    queue.removeFromDequeue("foo");

    assertThat(redis.lrange(queue.getDequeueName(), 0, -1)).containsExactly("foo", "bar").inOrder();
  }

  @Test
  public void removeAllShouldExclude() {
    RedisQueue queue = new RedisQueue(redis, "test");
    redis.lpush("test", "foo", "bar", "foo");

    queue.removeAll("foo");

    assertThat(redis.lrange("test", 0, -1)).containsExactly("bar");
  }

  @Test
  public void takeShouldPrependToDequeue() {
    RedisQueue queue = new RedisQueue(redis, "test");
    redis.lpush("test", "foo", "bar", "foo");
    redis.lpush(queue.getDequeueName(), "baz");

    String value = queue.take(Duration.ofMillis(1));

    assertThat(value).isEqualTo("foo");
    assertThat(redis.lrange("test", 0, -1)).containsExactly("foo", "bar").inOrder();
    assertThat(redis.lrange(queue.getDequeueName(), 0, -1)).containsExactly("foo", "baz").inOrder();
  }

  @Test
  public void takeEmptyShouldReturnNullAfterTimeoutAndIgnoreDequeue() {
    RedisQueue queue = new RedisQueue(redis, "test");
    redis.lpush(queue.getDequeueName(), "foo");

    String value = queue.take(Duration.ofMillis(1));

    assertThat(value).isNull();
    assertThat(redis.lrange("test", 0, -1)).isEmpty();
    assertThat(redis.lrange(queue.getDequeueName(), 0, -1)).containsExactly("foo");
  }

  @Test
  public void pollShouldPrependToDequeue() {
    RedisQueue queue = new RedisQueue(redis, "test");
    redis.lpush("test", "foo", "bar", "foo");
    redis.lpush(queue.getDequeueName(), "baz");

    String value = queue.poll();

    assertThat(value).isEqualTo("foo");
    assertThat(redis.lrange("test", 0, -1)).containsExactly("foo", "bar").inOrder();
    assertThat(redis.lrange(queue.getDequeueName(), 0, -1)).containsExactly("foo", "baz").inOrder();
  }

  @Test
  public void pollEmptyShouldReturnNullAndIgnoreDequeue() {
    RedisQueue queue = new RedisQueue(redis, "test");
    redis.lpush(queue.getDequeueName(), "foo");

    String value = queue.poll();

    assertThat(value).isNull();
    assertThat(redis.lrange("test", 0, -1)).isEmpty();
    assertThat(redis.lrange(queue.getDequeueName(), 0, -1)).containsExactly("foo");
  }

  @Test
  public void sizeYieldsLengthAndIgnoresDequeue() {
    RedisQueue queue = new RedisQueue(redis, "test");

    redis.lpush(queue.getDequeueName(), "foo");
    assertThat(queue.size()).isEqualTo(0);
    redis.lpush("test", "bar");
    assertThat(queue.size()).isEqualTo(1);
    redis.lpush("test", "baz");
    assertThat(queue.size()).isEqualTo(2);
  }

  private final Iterable<String> VISIT_ENTRIES = ImmutableList.of("one", "two", "three", "four");

  @Test
  public void visitShouldEnumerateAndIgnoreDequeue() {
    int listPageSize = 3;
    RedisQueue queue = new RedisQueue(redis, "test", listPageSize);
    redis.lpush(queue.getDequeueName(), "processing");
    for (String entry : VISIT_ENTRIES) {
      redis.lpush("test", entry);
    }
    StringVisitor visitor = mock(StringVisitor.class);

    queue.visit(visitor);

    for (String entry : VISIT_ENTRIES) {
      verify(visitor, times(1)).visit(entry);
    }
  }

  @Test
  public void visitDequeueShouldEnumerateAndIgnoreQueue() {
    int listPageSize = 3;
    RedisQueue queue = new RedisQueue(redis, "test", listPageSize);
    redis.lpush("test", "processing");
    for (String entry : VISIT_ENTRIES) {
      redis.lpush(queue.getDequeueName(), entry);
    }
    StringVisitor visitor = mock(StringVisitor.class);

    queue.visitDequeue(visitor);

    for (String entry : VISIT_ENTRIES) {
      verify(visitor, times(1)).visit(entry);
    }
  }
}
