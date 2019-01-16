// Copyright 2018 The Bazel Authors. All rights reserved.
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
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.mockito.Mockito.mock;
import static redis.clients.jedis.Protocol.Keyword.SUBSCRIBE;
import static redis.clients.jedis.Protocol.Keyword.UNSUBSCRIBE;

import build.buildfarm.instance.shard.OperationSubscriber.TimedWatchFuture;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Sets;
import com.google.longrunning.Operation;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import redis.clients.jedis.Client;

@RunWith(JUnit4.class)
public class OperationSubscriberTest {
  private static class UnobservableWatcher extends TimedWatcher<Operation> {
    UnobservableWatcher() {
      super(/* expiresAt=*/ Instant.now());
    }

    @Override
    public void observe(Operation operation) {
      throw new UnsupportedOperationException();
    }
  }

  private static class TestClient extends Client {
    private final Set<String> subscriptions = Sets.newHashSet();
    private final BlockingQueue<List<Object>> replyQueue = new LinkedBlockingQueue<>();
    private final List<List<Object>> pendingReplies = Lists.newArrayList();

    Set<String> getSubscriptions() {
      return subscriptions;
    }

    @Override
    public List<Object> getRawObjectMultiBulkReply() {
      try {
        return replyQueue.take();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }

    @Override
    public void subscribe(String... channels) {
      for (String channel : channels) {
        if (subscriptions.add(channel)) {
          pendingReplies.add(ImmutableList.of(SUBSCRIBE.raw, channel.getBytes(), (long) subscriptions.size()));
        } else {
          throw new IllegalStateException("subscribe to already subscribed channel: " + channel);
        }
      }
    }

    @Override
    public void unsubscribe() {
      long counter = subscriptions.size();
      for (String channel : subscriptions) {
        pendingReplies.add(ImmutableList.of(UNSUBSCRIBE.raw, channel.getBytes(), --counter));
      }
      subscriptions.clear();
    }

    @Override
    public void unsubscribe(String... channels) {
      for (String channel : channels) {
        if (subscriptions.remove(channel)) {
          pendingReplies.add(ImmutableList.of(UNSUBSCRIBE.raw, channel.getBytes(), (long) subscriptions.size()));
        } else {
          throw new IllegalStateException("unsubscribe from unknown channel: " + channel);
        }
      }
    }

    @Override
    public void flush() {
      replyQueue.addAll(pendingReplies);
      pendingReplies.clear();
    }
  };

  @Test
  public void novelChannelWatcherSubscribes() throws InterruptedException {
    ListMultimap<String, TimedWatchFuture> watchers = 
        Multimaps.<String, TimedWatchFuture>synchronizedListMultimap(
            MultimapBuilder.linkedHashKeys().arrayListValues().build());
    ExecutorService subscriberService = newDirectExecutorService();
    OperationSubscriber operationSubscriber = new OperationSubscriber(watchers, subscriberService) {
      @Override
      protected Instant nextExpiresAt() {
        throw new UnsupportedOperationException();
      }
    };

    TestClient testClient = new TestClient();
    Thread proceedThread = new Thread(() -> operationSubscriber.proceed(testClient));
    proceedThread.start();
    // ensure that the client is subscribed
    while (testClient.getSubscriptions().isEmpty()) {
      MICROSECONDS.sleep(10);
    }

    String novelChannel = "novel-channel";
    TimedWatcher<Operation> novelWatcher = new UnobservableWatcher();
    operationSubscriber.watch(novelChannel, novelWatcher);
    assertThat(Iterables.getOnlyElement(watchers.get(novelChannel)).getWatcher()).isEqualTo(novelWatcher);
    String[] channels = new String[1];
    channels[0] = novelChannel;
    assertThat(testClient.getSubscriptions()).contains(novelChannel);
    operationSubscriber.unsubscribe();
    proceedThread.join();
  }

  @Test
  public void existingChannelWatcherSuppressesSubscription() {
    ListMultimap<String, TimedWatchFuture> watchers = 
        MultimapBuilder.linkedHashKeys().arrayListValues().build();
    ExecutorService subscriberService = newDirectExecutorService();
    OperationSubscriber operationSubscriber = new OperationSubscriber(watchers, subscriberService) {
      @Override
      protected Instant nextExpiresAt() {
        throw new UnsupportedOperationException();
      }
    };
    String existingChannel = "existing-channel";
    TimedWatcher<Operation> existingWatcher = new UnobservableWatcher();
    watchers.put(existingChannel, new TimedWatchFuture(existingWatcher) {
      @Override
      public void unwatch() {
        throw new UnsupportedOperationException();
      }
    });
    TimedWatcher<Operation> novelWatcher = new UnobservableWatcher();
    operationSubscriber.watch(existingChannel, novelWatcher);
    assertThat(watchers.get(existingChannel).size()).isEqualTo(2);
  }

  @Test
  public void nullMessageUnsubscribesWatcher() throws InterruptedException {
    ListMultimap<String, TimedWatchFuture> watchers = 
        Multimaps.<String, TimedWatchFuture>synchronizedListMultimap(
            MultimapBuilder.linkedHashKeys().arrayListValues().build());
    ExecutorService subscriberService = newDirectExecutorService();
    OperationSubscriber operationSubscriber = new OperationSubscriber(watchers, subscriberService) {
      @Override
      protected Instant nextExpiresAt() {
        return Instant.now();
      }
    };

    TestClient testClient = new TestClient();
    Thread proceedThread = new Thread(() -> operationSubscriber.proceed(testClient));
    proceedThread.start();
    // ensure that the client is subscribed
    while (testClient.getSubscriptions().isEmpty()) {
      MICROSECONDS.sleep(10);
    }

    String nullMessageChannel = "null-message-channel";
    TimedWatcher<Operation> nullMessageWatcher = new TimedWatcher<Operation>(Instant.now()) {
      @Override
      public void observe(Operation operation) {
        if (operation != null) {
          throw new UnsupportedOperationException();
        }
      }
    };
    operationSubscriber.watch(nullMessageChannel, nullMessageWatcher);
    operationSubscriber.onMessage(nullMessageChannel, /* message=*/ null);
    assertThat(testClient.getSubscriptions()).doesNotContain(nullMessageChannel);
    operationSubscriber.unsubscribe();
    proceedThread.join();
  }
}
