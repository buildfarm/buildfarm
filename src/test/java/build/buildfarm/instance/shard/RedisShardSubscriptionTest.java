// Copyright 2018 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.instance.shard;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import build.buildfarm.common.function.InterruptingRunnable;
import build.buildfarm.common.redis.RedisClient;
import build.buildfarm.instance.shard.RedisShardSubscriber.TimedWatchFuture;
import build.buildfarm.instance.shard.codec.json.JsonCodec;
import com.github.fppt.jedismock.RedisServer;
import com.github.fppt.jedismock.operations.server.MockExecutor;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.ServiceOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Multimaps;
import com.google.common.util.concurrent.SettableFuture;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.function.Consumer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.exceptions.JedisException;

@RunWith(JUnit4.class)
public class RedisShardSubscriptionTest {
  private RedisServer server;

  RedisShardSubscriber getDefaultRedisSubscriber() {
    ListMultimap<String, TimedWatchFuture> watchers =
        Multimaps.synchronizedListMultimap(
            MultimapBuilder.linkedHashKeys().arrayListValues().build());
    return new RedisShardSubscriber(
        watchers, null, 1, "worker-channel", workerName -> {}, null, JsonCodec.CODEC);
  }

  /**
   * Creates a jedis-mock server bound explicitly to the IPv4 loopback address. Using the no-arg
   * {@code newRedisServer()} causes {@code getHost()} to return "localhost", which on macOS may
   * resolve to {@code ::1} (IPv6) while the server socket is IPv4-only, resulting in the Jedis
   * client connecting to a different protocol stack than the server is listening on.
   */
  private RedisServer startServer() throws IOException {
    server = RedisServer.newRedisServer(0, InetAddress.getByName("localhost")).start();
    return server;
  }

  private RedisServer startServer(ServiceOptions options) throws IOException {
    server =
        RedisServer.newRedisServer(0, InetAddress.getByName("localhost"))
            .setOptions(options)
            .start();
    return server;
  }

  private UnifiedJedis createJedis(RedisServer server) {
    return new UnifiedJedis(new HostAndPort(server.getHost(), server.getBindPort()));
  }

  @After
  public void tearDown() throws IOException {
    if (server != null) {
      server.stop();
      server = null;
    }
  }

  @Test
  public void runReturnsWhenStopped() throws Exception {
    RedisServer server = startServer();

    InterruptingRunnable onUnsubscribe = mock(InterruptingRunnable.class);
    Consumer<UnifiedJedis> onReset = mock(Consumer.class);
    List<String> subscriptions = ImmutableList.of("test");
    UnifiedJedis jedis = createJedis(server);
    RedisShardSubscriber redisSubscriber = getDefaultRedisSubscriber();

    RedisShardSubscription subscription =
        new RedisShardSubscription(
            redisSubscriber, onUnsubscribe, onReset, () -> subscriptions, new RedisClient(jedis));
    final long subscribeCheckTime = 100;

    Thread thread = new Thread(subscription);
    thread.start();

    while (!redisSubscriber.checkIfSubscribed(subscribeCheckTime)) {
      Thread.yield();
    }

    subscription.stop();

    thread.join();

    verifyNoInteractions(onUnsubscribe);
    verifyNoInteractions(onReset);
  }

  @Test
  public void onResetWhenUnavailable() throws Exception {
    SettableFuture<Void> broken = SettableFuture.create();
    RedisServer server =
        startServer(
            ServiceOptions.withInterceptor(
                (state, roName, params) -> {
                  if (roName.equalsIgnoreCase("subscribe") && !broken.isDone()) {
                    broken.set(null);
                    return MockExecutor.breakConnection(state);
                  }
                  return MockExecutor.proceed(state, roName, params);
                }));
    SettableFuture<Void> reset = SettableFuture.create();
    InterruptingRunnable onUnsubscribe = mock(InterruptingRunnable.class);
    Consumer<UnifiedJedis> onReset = mock(Consumer.class);
    doAnswer(
            invocation -> {
              reset.set(null);
              return null;
            })
        .when(onReset)
        .accept(any(UnifiedJedis.class));
    List<String> subscriptions = ImmutableList.of("test");
    UnifiedJedis jedis = createJedis(server);

    RedisShardSubscription subscription =
        new RedisShardSubscription(
            getDefaultRedisSubscriber(),
            onUnsubscribe,
            onReset,
            () -> subscriptions,
            new RedisClient(jedis));

    Thread thread = new Thread(subscription);
    thread.start();

    reset.get();
    subscription.stop();

    thread.join();

    verifyNoInteractions(onUnsubscribe);
    verify(onReset, times(1)).accept(jedis);
  }

  @Test
  public void exceptionOnStopTImeout() throws Exception {
    SettableFuture<Void> broken = SettableFuture.create();
    RedisServer server =
        startServer(
            ServiceOptions.withInterceptor(
                (state, roName, params) -> {
                  if (roName.equalsIgnoreCase("subscribe") && !broken.isDone()) {
                    broken.set(null);
                    return MockExecutor.breakConnection(state);
                  }
                  return MockExecutor.proceed(state, roName, params);
                }));
    SettableFuture<Void> reset = SettableFuture.create();
    InterruptingRunnable onUnsubscribe = mock(InterruptingRunnable.class);
    Consumer<UnifiedJedis> onReset = mock(Consumer.class);
    doAnswer(
            invocation -> {
              reset.set(null);
              return null;
            })
        .when(onReset)
        .accept(any(UnifiedJedis.class));
    List<String> subscriptions = ImmutableList.of("test");
    UnifiedJedis jedis = createJedis(server);

    RedisShardSubscription subscription =
        new RedisShardSubscription(
            getDefaultRedisSubscriber(),
            onUnsubscribe,
            onReset,
            () -> subscriptions,
            new RedisClient(jedis));

    Thread thread = new Thread(subscription);
    thread.start();

    reset.get();

    try {
      subscription.stop(0);
    } catch (Exception e) {
      assert e.getClass() == UnsubscribeTimeoutException.class;
    }

    subscription.stop(1000);

    thread.join();

    verifyNoInteractions(onUnsubscribe);
    verify(onReset, times(1)).accept(jedis);
  }

  @Test
  public void exceptionOnStopWhenNotSubscribed() throws Exception {
    SettableFuture<Void> broken = SettableFuture.create();
    RedisServer server =
        startServer(
            ServiceOptions.withInterceptor(
                (state, roName, params) -> {
                  if (roName.equalsIgnoreCase("subscribe") && !broken.isDone()) {
                    broken.set(null);
                    return MockExecutor.breakConnection(state);
                  }
                  return MockExecutor.proceed(state, roName, params);
                }));
    InterruptingRunnable onUnsubscribe = mock(InterruptingRunnable.class);
    Consumer<UnifiedJedis> onReset = mock(Consumer.class);
    List<String> subscriptions = ImmutableList.of("test");
    UnifiedJedis jedis = createJedis(server);
    RedisShardSubscriber redisSubscriber = getDefaultRedisSubscriber();

    RedisShardSubscription subscription =
        new RedisShardSubscription(
            redisSubscriber, onUnsubscribe, onReset, () -> subscriptions, new RedisClient(jedis));

    Assert.assertThrows(JedisException.class, () -> subscription.stop());

    verifyNoInteractions(onUnsubscribe);
    verifyNoInteractions(onReset);

    // Subscription does not complete
    assert !redisSubscriber.isSubscribed();
  }

  @Test
  public void onUnsubscribeOnRecognizedException() throws Exception {
    RedisServer server =
        startServer(
            ServiceOptions.withInterceptor(
                (state, roName, params) -> {
                  if (roName.equalsIgnoreCase("subscribe")) {
                    return Response.error("unknown");
                  }
                  return MockExecutor.proceed(state, roName, params);
                }));
    InterruptingRunnable onUnsubscribe = mock(InterruptingRunnable.class);
    Consumer<UnifiedJedis> onReset = mock(Consumer.class);
    List<String> subscriptions = ImmutableList.of("test");
    UnifiedJedis jedis = createJedis(server);

    RedisShardSubscription subscription =
        new RedisShardSubscription(
            getDefaultRedisSubscriber(),
            onUnsubscribe,
            onReset,
            () -> subscriptions,
            new RedisClient(jedis));

    subscription.run();

    verify(onUnsubscribe, times(1)).runInterruptibly();
    verifyNoInteractions(onReset);
  }

  @Test
  public void threadInterruptedIfOnUnsubscribedInterrupted() throws Exception {
    RedisServer server =
        startServer(
            ServiceOptions.withInterceptor(
                (state, roName, params) -> {
                  if (roName.equalsIgnoreCase("subscribe")) {
                    return Response.error("unknown");
                  }
                  return MockExecutor.proceed(state, roName, params);
                }));
    InterruptingRunnable onUnsubscribe =
        () -> {
          throw new InterruptedException();
        };
    Consumer<UnifiedJedis> onReset = mock(Consumer.class);
    List<String> subscriptions = ImmutableList.of("test");
    UnifiedJedis jedis = createJedis(server);

    RedisShardSubscription subscription =
        new RedisShardSubscription(
            getDefaultRedisSubscriber(),
            onUnsubscribe,
            onReset,
            () -> subscriptions,
            new RedisClient(jedis));

    subscription.run();

    assertThat(Thread.currentThread().isInterrupted()).isTrue();
    verifyNoInteractions(onReset);
  }
}
