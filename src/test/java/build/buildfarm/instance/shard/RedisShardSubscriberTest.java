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

import static build.buildfarm.instance.shard.RedisShardBackplane.printOperationChange;
import static build.buildfarm.instance.shard.RedisShardBackplane.toTimestamp;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static redis.clients.jedis.Protocol.Command.SUBSCRIBE;
import static redis.clients.jedis.Protocol.Command.UNSUBSCRIBE;
import static redis.clients.jedis.Protocol.ResponseKeyword;

import build.buildfarm.instance.shard.RedisShardSubscriber.TimedWatchFuture;
import build.buildfarm.v1test.OperationChange;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.common.truth.Correspondence;
import com.google.longrunning.Operation;
import com.google.protobuf.InvalidProtocolBufferException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import redis.clients.jedis.CommandArguments;
import redis.clients.jedis.Connection;
import redis.clients.jedis.args.Rawable;
import redis.clients.jedis.args.RawableFactory;
import redis.clients.jedis.commands.ProtocolCommand;

@RunWith(JUnit4.class)
public class RedisShardSubscriberTest {
  /* he cannot unsee */
  private static class LidlessTimedWatchFuture extends TimedWatchFuture {
    LidlessTimedWatchFuture(TimedWatcher timedWatcher) {
      super(timedWatcher);
    }

    @Override
    public void unwatch() {
      throw new UnsupportedOperationException();
    }
  }

  private static class TestConnection extends Connection {
    private final Set<Rawable> subscriptions = Sets.newConcurrentHashSet();
    private final BlockingQueue<Runnable> pendingRequests = new LinkedBlockingQueue<>();
    private final BlockingQueue<Object> replyQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<Object> pendingReplies = new LinkedBlockingQueue<>();

    Set<Rawable> getSubscriptions() {
      return subscriptions;
    }

    @Override
    public List<Object> getUnflushedObjectMultiBulkReply() {
      throw new UnsupportedOperationException("getUnflushedObjectMultiBulkReply is deprecated");
    }

    @Override
    public Object getUnflushedObject() {
      try {
        return replyQueue.take();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }

    @Override
    public void sendCommand(final CommandArguments cargs) {
      ProtocolCommand command = cargs.getCommand();
      if (command == SUBSCRIBE) {
        pendingRequests.add(() -> subscribe(cargs));
      } else if (command == UNSUBSCRIBE) {
        if (cargs.size() == 1) {
          // only includes command
          pendingRequests.add(() -> unsubscribe());
        } else {
          pendingRequests.add(() -> unsubscribe(cargs));
        }
      } else {
        throw new UnsupportedOperationException(cargs.toString());
      }
    }

    @Override
    public void setTimeoutInfinite() {
      // ignore
    }

    private void subscribe(Iterable<Rawable> channels) {
      boolean isCommand = true;
      Rawable command = null;
      for (Rawable channel : channels) {
        if (isCommand) {
          isCommand = false;
        } else {
          if (subscriptions.add(channel)) {
            pendingReplies.add(
                ImmutableList.of(
                    ResponseKeyword.SUBSCRIBE.getRaw(),
                    channel.getRaw(),
                    (long) subscriptions.size()));
          } else {
            throw new IllegalStateException("subscribe to already subscribed channel: " + channel);
          }
        }
      }
    }

    private void unsubscribe() {
      long counter = subscriptions.size();
      for (Rawable channel : subscriptions) {
        pendingReplies.add(
            ImmutableList.of(ResponseKeyword.UNSUBSCRIBE.getRaw(), channel.getRaw(), --counter));
      }
      subscriptions.clear();
    }

    private void unsubscribe(Iterable<Rawable> channels) {
      boolean isCommand = true;
      Rawable command = null;
      for (Rawable channel : channels) {
        if (isCommand) {
          isCommand = false;
        } else {
          if (subscriptions.remove(channel)) {
            pendingReplies.add(
                ImmutableList.of(
                    ResponseKeyword.UNSUBSCRIBE.getRaw(),
                    channel.getRaw(),
                    (long) subscriptions.size()));
          } else {
            throw new IllegalStateException("unsubscribe from unknown channel: " + channel);
          }
        }
      }
    }

    @Override
    public void flush() {
      for (Runnable request = pendingRequests.poll();
          request != null;
          request = pendingRequests.poll()) {
        request.run();
      }
      pendingReplies.drainTo(replyQueue);
    }
  }

  RedisShardSubscriber createSubscriber(
      ListMultimap<String, TimedWatchFuture> watchers, Executor executor) {
    return new RedisShardSubscriber(watchers, /* workers=*/ null, "worker-channel", executor);
  }

  RedisShardSubscriber createSubscriber(ListMultimap<String, TimedWatchFuture> watchers) {
    return createSubscriber(watchers, /* executor=*/ null);
  }

  private static final Correspondence<Rawable, Rawable> rawableCorrespondence =
      Correspondence.from(
          new Correspondence.BinaryPredicate<Rawable, Rawable>() {
            @Override
            public boolean apply(Rawable a, Rawable e) {
              return Arrays.equals(a.getRaw(), e.getRaw());
            }
          },
          "is rawably equivalent to");

  @Test
  public void novelChannelWatcherSubscribes() throws InterruptedException {
    ListMultimap<String, TimedWatchFuture> watchers =
        Multimaps.synchronizedListMultimap(
            MultimapBuilder.linkedHashKeys().arrayListValues().build());
    RedisShardSubscriber operationSubscriber = createSubscriber(watchers, directExecutor());

    TestConnection testConnection = new TestConnection();
    Thread proceedThread = new Thread(() -> operationSubscriber.start(testConnection));
    proceedThread.start();
    while (!operationSubscriber.isSubscribed()) {
      MICROSECONDS.sleep(10);
    }

    String novelChannel = "novel-channel";
    TimedWatcher novelWatcher = new UnobservableWatcher();
    operationSubscriber.watch(novelChannel, novelWatcher);
    assertThat(Iterables.getOnlyElement(watchers.get(novelChannel)).getWatcher())
        .isEqualTo(novelWatcher);
    String[] channels = new String[1];
    channels[0] = novelChannel;
    assertThat(testConnection.getSubscriptions())
        .comparingElementsUsing(rawableCorrespondence)
        .contains(RawableFactory.from(novelChannel));
    operationSubscriber.unsubscribe();
    proceedThread.join();
  }

  @Test
  public void watchedOperationChannelsReflectsWatchers() {
    ListMultimap<String, TimedWatchFuture> watchers =
        MultimapBuilder.linkedHashKeys().arrayListValues().build();
    RedisShardSubscriber operationSubscriber = createSubscriber(watchers);
    assertThat(operationSubscriber.watchedOperationChannels()).isEmpty();
    String addedChannel = "added-channel";
    watchers.put(addedChannel, null);
    assertThat(operationSubscriber.watchedOperationChannels()).containsExactly(addedChannel);
    watchers.removeAll(addedChannel);
    assertThat(operationSubscriber.watchedOperationChannels()).isEmpty();
  }

  @Test
  public void expiredWatchedOperationChannelsReflectsWatchers() {
    ListMultimap<String, TimedWatchFuture> watchers =
        MultimapBuilder.linkedHashKeys().arrayListValues().build();
    RedisShardSubscriber operationSubscriber = createSubscriber(watchers);

    TimedWatcher unexpiredWatcher = new UnobservableWatcher(Instant.MAX);
    TimedWatcher expiredWatcher = new UnobservableWatcher(Instant.EPOCH);
    Instant now = Instant.now();
    // EPOCH < now < MAX

    assertThat(operationSubscriber.expiredWatchedOperationChannels(now)).isEmpty();

    String unexpiredChannel = "channel-with-unexpired-watcher";
    watchers.put(unexpiredChannel, new LidlessTimedWatchFuture(unexpiredWatcher));
    assertThat(operationSubscriber.expiredWatchedOperationChannels(now)).isEmpty();

    String expiredChannel = "channel-with-expired-watcher";
    watchers.put(expiredChannel, new LidlessTimedWatchFuture(expiredWatcher));
    assertThat(operationSubscriber.expiredWatchedOperationChannels(now))
        .containsExactly(expiredChannel);

    String mixedChannel = "channel-with-some-expired-watchers";
    watchers.put(mixedChannel, new LidlessTimedWatchFuture(unexpiredWatcher));
    watchers.put(mixedChannel, new LidlessTimedWatchFuture(expiredWatcher));
    watchers.put(mixedChannel, new LidlessTimedWatchFuture(expiredWatcher));
    watchers.put(mixedChannel, new LidlessTimedWatchFuture(unexpiredWatcher));
    assertThat(operationSubscriber.expiredWatchedOperationChannels(now))
        .containsExactly(expiredChannel, mixedChannel);

    watchers.removeAll(expiredChannel);
    assertThat(operationSubscriber.expiredWatchedOperationChannels(now))
        .containsExactly(mixedChannel);

    watchers.removeAll(mixedChannel);
    assertThat(operationSubscriber.expiredWatchedOperationChannels(now)).isEmpty();
  }

  @Test
  public void existingChannelWatcherSuppressesSubscription() {
    ListMultimap<String, TimedWatchFuture> watchers =
        MultimapBuilder.linkedHashKeys().arrayListValues().build();
    RedisShardSubscriber operationSubscriber = createSubscriber(watchers, directExecutor());
    String existingChannel = "existing-channel";
    TimedWatcher existingWatcher = new UnobservableWatcher();
    watchers.put(existingChannel, new LidlessTimedWatchFuture(existingWatcher));
    TimedWatcher novelWatcher = new UnobservableWatcher();
    operationSubscriber.watch(existingChannel, novelWatcher);
    assertThat(watchers.get(existingChannel).size()).isEqualTo(2);
  }

  @Test
  public void doneResetOperationIsObservedAndUnsubscribed()
      throws InterruptedException, InvalidProtocolBufferException {
    ListMultimap<String, TimedWatchFuture> watchers =
        Multimaps.synchronizedListMultimap(
            MultimapBuilder.linkedHashKeys().arrayListValues().build());
    RedisShardSubscriber operationSubscriber = createSubscriber(watchers, directExecutor());

    TestConnection testConnection = new TestConnection();
    Thread proceedThread = new Thread(() -> operationSubscriber.start(testConnection));
    proceedThread.start();
    while (!operationSubscriber.isSubscribed()) {
      MICROSECONDS.sleep(10);
    }

    String doneMessageChannel = "done-message-channel";
    AtomicBoolean observed = new AtomicBoolean(false);
    TimedWatcher doneMessageWatcher =
        new TimedWatcher(Instant.now()) {
          @Override
          public void observe(Operation operation) {
            if (operation.getDone()) {
              observed.set(true);
            }
          }
        };
    operationSubscriber.watch(doneMessageChannel, doneMessageWatcher);
    operationSubscriber.onMessage(
        doneMessageChannel,
        printOperationChange(
            OperationChange.newBuilder()
                .setReset(
                    OperationChange.Reset.newBuilder()
                        .setOperation(Operation.newBuilder().setDone(true).build())
                        .build())
                .build()));
    assertThat(observed.get()).isTrue();
    assertThat(testConnection.getSubscriptions()).doesNotContain(doneMessageChannel);
    operationSubscriber.unsubscribe();
    proceedThread.join();
  }

  @Test
  public void shouldResetWatchers() {
    ListMultimap<String, TimedWatchFuture> watchers =
        MultimapBuilder.linkedHashKeys().arrayListValues().build();
    TimedWatcher resetWatcher = new UnobservableWatcher(Instant.EPOCH);

    Instant now = Instant.now();
    assertThat(resetWatcher.isExpiredAt(now)).isTrue();

    String resetChannel = "reset-channel";
    watchers.put(resetChannel, new LidlessTimedWatchFuture(resetWatcher));

    RedisShardSubscriber operationSubscriber = createSubscriber(watchers);
    operationSubscriber.resetWatchers(resetChannel, Instant.MAX);
    assertThat(resetWatcher.isExpiredAt(now)).isFalse();
  }

  @Test
  public void terminatesExpiredWatchersOnExpireMessage() throws InvalidProtocolBufferException {
    ListMultimap<String, TimedWatchFuture> watchers =
        MultimapBuilder.linkedHashKeys().arrayListValues().build();
    TimedWatcher expiredWatcher = mock(TimedWatcher.class);
    when(expiredWatcher.isExpiredAt(any(Instant.class))).thenReturn(true);

    RedisShardSubscriber operationSubscriber = createSubscriber(watchers, directExecutor());

    String expireChannel = "expire-channel";
    TimedWatchFuture watchFuture =
        new TimedWatchFuture(expiredWatcher) {
          @Override
          public void unwatch() {
            operationSubscriber.unwatch(expireChannel, this);
          }
        };
    watchers.put(expireChannel, watchFuture);

    operationSubscriber.onMessage(
        expireChannel,
        printOperationChange(
            OperationChange.newBuilder()
                .setEffectiveAt(toTimestamp(Instant.now()))
                .setExpire(OperationChange.Expire.newBuilder().setForce(false).build())
                .build()));
    verify(expiredWatcher, times(1)).observe(null);
    assertThat(watchers.get(expireChannel)).isEmpty();
  }

  @Test
  public void unsetTypeOperationChangeIsIgnored() {
    ListMultimap<String, TimedWatchFuture> watchers =
        MultimapBuilder.linkedHashKeys().arrayListValues().build();
    RedisShardSubscriber operationSubscriber = createSubscriber(watchers, directExecutor());

    operationSubscriber.onOperationChange(
        "unset-type-operation", OperationChange.getDefaultInstance());
  }

  @Test
  public void invalidOperationChangeIsIgnored() {
    ListMultimap<String, TimedWatchFuture> watchers =
        MultimapBuilder.linkedHashKeys().arrayListValues().build();
    RedisShardSubscriber operationSubscriber = createSubscriber(watchers, directExecutor());

    operationSubscriber.onMessage("invalid-operation-change", "not-json!#?");
  }
}
