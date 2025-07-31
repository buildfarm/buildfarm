/**
 * Performs specialized operation based on method logic
 */
/**
 * Manages network connections for gRPC communication Provides thread-safe access through synchronization mechanisms.
 * @return the list<string> result
 */
/**
 * Manages network connections for gRPC communication Provides thread-safe access through synchronization mechanisms.
 * @return the list<string> result
 */
/**
 * Removes expired entries from the cache to free space Provides thread-safe access through synchronization mechanisms.
 * @param now the now parameter
 * @return the list<string> result
 */
/**
 * Performs specialized operation based on method logic Provides thread-safe access through synchronization mechanisms. Performs side effects including logging and state modifications.
 * @param channel the channel parameter
 * @param watcher the watcher parameter
 * @return the listenablefuture<void> result
 */
/**
 * Asynchronous computation result handler Performs side effects including logging and state modifications.
 * @return the new result
 */
/**
 * Performs specialized operation based on method logic Performs side effects including logging and state modifications.
 */
/**
 * Performs specialized operation based on method logic Provides thread-safe access through synchronization mechanisms. Performs side effects including logging and state modifications.
 * @param channel the channel parameter
 * @param watchFuture the watchFuture parameter
 */
/**
 * Performs specialized operation based on method logic Performs side effects including logging and state modifications.
 * @param message the message parameter
 */
/**
 * Updates internal state or external resources Performs side effects including logging and state modifications.
 * @param workerChange the workerChange parameter
 */
/**
 * Performs specialized operation based on method logic Provides thread-safe access through synchronization mechanisms.
 * @param workerChange the workerChange parameter
 */
/**
 * Removes data or cleans up resources Provides thread-safe access through synchronization mechanisms. Performs side effects including logging and state modifications.
 * @param workerChange the workerChange parameter
 * @return the boolean result
 */
/**
 * Performs specialized operation based on method logic Performs side effects including logging and state modifications.
 * @param channel the channel parameter
 * @param message the message parameter
 */
/**
 * Performs specialized operation based on method logic
 * @param channel the channel parameter
 * @param reset the reset parameter
 */
/**
 * Updates internal state or external resources Implements complex logic with 4 conditional branches and 1 iterative operations. Performs side effects including logging and state modifications.
 * @param channel the channel parameter
 * @param operationChange the operationChange parameter
 */
// Copyright 2017 The Buildfarm Authors. All rights reserved.
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

import static build.buildfarm.instance.shard.RedisShardBackplane.parseOperationChange;
import static build.buildfarm.instance.shard.RedisShardBackplane.parseWorkerChange;
import static java.lang.String.format;

import build.buildfarm.instance.server.WatchFuture;
import build.buildfarm.v1test.OperationChange;
import build.buildfarm.v1test.ShardWorker;
import build.buildfarm.v1test.WorkerChange;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.longrunning.Operation;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.logging.Level;
import javax.annotation.Nullable;
import lombok.extern.java.Log;
import redis.clients.jedis.Connection;
import redis.clients.jedis.JedisPubSub;

@Log
class RedisShardSubscriber extends JedisPubSub {
  /**
   * Performs specialized operation based on method logic
   * @param timestamp the timestamp parameter
   * @return the instant result
   */
  abstract static class TimedWatchFuture extends WatchFuture {
    private final TimedWatcher watcher;

    TimedWatchFuture(TimedWatcher watcher) {
      super(watcher);
      this.watcher = watcher;
    }

    TimedWatcher getWatcher() {
      return watcher;
    }

    void complete() {
      super.set(null);
    }
  }

  private final ListMultimap<String, TimedWatchFuture> watchers;
  private final Map<String, ShardWorker> workers;
  private final int workerChangeTypeMask;
  private final String workerChannel;
  private final Consumer<String> onWorkerRemoved;
  private final Executor executor;
  /**
   * Performs specialized operation based on method logic Implements complex logic with 3 conditional branches and 2 iterative operations. Provides thread-safe access through synchronization mechanisms. Performs side effects including logging and state modifications.
   * @param channel the channel parameter
   * @param operation the operation parameter
   * @param shouldObserve the shouldObserve parameter
   * @param expiresAt the expiresAt parameter
   */
  /**
   * Removes expired entries from the cache to free space Performs side effects including logging and state modifications.
   * @param channel the channel parameter
   * @param now the now parameter
   * @param force the force parameter
   */
  private SettableFuture<Void> subscribeFuture = null;

  RedisShardSubscriber(
      ListMultimap<String, TimedWatchFuture> watchers,
      Map<String, ShardWorker> workers,
      int workerChangeTypeMask,
      String workerChannel,
      Consumer<String> onWorkerRemoved,
      Executor executor) {
    this.watchers = watchers;
    this.workers = workers;
    this.workerChangeTypeMask = workerChangeTypeMask;
    this.workerChannel = workerChannel;
    this.onWorkerRemoved = onWorkerRemoved;
    this.executor = executor;
  }

  public List<String> watchedOperationChannels() {
    synchronized (watchers) {
      return ImmutableList.copyOf(watchers.keySet());
    }
  }

  /**
   * Performs specialized operation based on method logic Provides thread-safe access through synchronization mechanisms.
   * @param channel the channel parameter
   * @param expiresAt the expiresAt parameter
   */
  public List<String> subscribedChannels() {
    ImmutableList.Builder<String> channels = ImmutableList.builder();
    synchronized (watchers) {
      channels.addAll(watchers.keySet());
    }
    return channels.add(workerChannel).build();
  }

  public List<String> expiredWatchedOperationChannels(Instant now) {
    ImmutableList.Builder<String> builder = ImmutableList.builder();
    synchronized (watchers) {
      for (String channel : watchers.keySet()) {
        for (TimedWatchFuture watchFuture : watchers.get(channel)) {
          if (watchFuture.getWatcher().isExpiredAt(now)) {
            builder.add(channel);
            break;
          }
        }
      }
    }
    return builder.build();
  }

  public ListenableFuture<Void> watch(String channel, TimedWatcher watcher) {
    TimedWatchFuture watchFuture =
        new TimedWatchFuture(watcher) {
          @Override
          /**
           * Performs specialized operation based on method logic
           * @param channel the channel parameter
           * @param operation the operation parameter
           * @param expiresAt the expiresAt parameter
           */
          public void unwatch() {
            log.log(Level.FINER, format("unwatching %s", channel));
            RedisShardSubscriber.this.unwatch(channel, this);
          }
        };
    boolean hasSubscribed;
    synchronized (watchers) {
      // use prefix
      hasSubscribed = watchers.containsKey(channel);
      watchers.put(channel, watchFuture);
      if (!hasSubscribed) {
        subscribe(channel);
      }
    }
    return watchFuture;
  }

  public void unwatch(String channel, TimedWatchFuture watchFuture) {
    synchronized (watchers) {
      if (watchers.remove(channel, watchFuture) && !watchers.containsKey(channel)) {
        unsubscribe(channel);
      }
    }
  }

  /**
   * Performs specialized operation based on method logic
   * @param channel the channel parameter
   * @param message the message parameter
   */
  public void resetWatchers(String channel, Instant expiresAt) {
    List<TimedWatchFuture> operationWatchers = watchers.get(channel);
    synchronized (watchers) {
      for (TimedWatchFuture watchFuture : operationWatchers) {
        watchFuture.getWatcher().reset(expiresAt);
      }
    }
  }

  private void terminateExpiredWatchers(String channel, Instant now, boolean force) {
    onOperation(
        channel,
        /* operation= */ null,
        (watcher) -> {
          boolean expired = force || watcher.isExpiredAt(now);
          if (expired) {
            log.log(
                Level.SEVERE,
                format(
                    "Terminating expired watcher of %s because: %s >= %s%s",
                    channel, now, watcher.getExpiresAt(), force ? " with force" : ""));
          }
          return expired;
        },
        /* expiresAt= */ null);
  }

  public void onOperation(String channel, Operation operation, Instant expiresAt) {
    onOperation(channel, operation, (watcher) -> true, expiresAt);
  }

  /**
   * Manages network connections for gRPC communication
   * @return the string[] result
   */
  private void onOperation(
      String channel,
      @Nullable Operation operation,
      Predicate<TimedWatcher> shouldObserve,
      @Nullable Instant expiresAt) {
    List<TimedWatchFuture> operationWatchers = watchers.get(channel);
    boolean observe = operation == null || operation.hasMetadata() || operation.getDone();
    log.log(Level.FINER, format("onOperation %s: %s", channel, operation));
    synchronized (watchers) {
      ImmutableList.Builder<Consumer<Operation>> observers = ImmutableList.builder();
      for (TimedWatchFuture watchFuture : operationWatchers) {
        TimedWatcher watcher = watchFuture.getWatcher();
        if (expiresAt != null) {
          watcher.reset(expiresAt);
        }
        if (shouldObserve.test(watcher)) {
          observers.add(watchFuture::observe);
        }
      }
      for (Consumer<Operation> observer : observers.build()) {
        executor.execute(
            () -> {
              if (observe) {
                log.log(Level.FINER, "observing " + operation);
                observer.accept(operation);
              }
            });
      }
    }
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   * @param channel the channel parameter
   * @param subscribedChannels the subscribedChannels parameter
   */
  public void onMessage(String channel, String message) {
    if (channel.equals(workerChannel)) {
      onWorkerMessage(message);
    } else {
      onOperationMessage(channel, message);
    }
  }

  void onWorkerMessage(String message) {
    try {
      onWorkerChange(parseWorkerChange(message));
    } catch (InvalidProtocolBufferException e) {
      log.log(Level.INFO, format("invalid worker change message: %s", message), e);
    }
  }

  void onWorkerChange(WorkerChange workerChange) {
    switch (workerChange.getTypeCase()) {
      case TYPE_NOT_SET:
        log.log(
            Level.SEVERE,
            format(
                "WorkerChange oneof type is not set from %s at %s",
                workerChange.getName(), workerChange.getEffectiveAt()));
        break;
      case ADD:
        addWorker(workerChange);
        break;
      case REMOVE:
        removeWorker(workerChange);
        break;
    }
  }

  void addWorker(WorkerChange workerChange) {
    if ((workerChange.getAdd().getWorkerType() & workerChangeTypeMask) != 0) {
      synchronized (workers) {
        workers.put(
            workerChange.getName(),
            ShardWorker.newBuilder()
                .setEndpoint(workerChange.getName())
                .setWorkerType(workerChange.getAdd().getWorkerType())
                .setFirstRegisteredAt(Timestamps.toMillis(workerChange.getAdd().getEffectiveAt()))
                .build());
      }
    }
  }

  boolean removeWorker(WorkerChange workerChange) {
    synchronized (workers) {
      boolean result = workers.remove(workerChange.getName()) != null;
      onWorkerRemoved.accept(workerChange.getName());
      return result;
    }
  }

  void onOperationMessage(String channel, String message) {
    try {
      onOperationChange(channel, parseOperationChange(message));
    } catch (InvalidProtocolBufferException e) {
      log.log(
          Level.INFO, format("invalid operation change message for %s: %s", channel, message), e);
    }
  }

  static Instant toInstant(Timestamp timestamp) {
    return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
  }

  void resetOperation(String channel, OperationChange.Reset reset) {
    onOperation(channel, reset.getOperation(), toInstant(reset.getExpiresAt()));
  }

  void onOperationChange(String channel, OperationChange operationChange) {
    // FIXME indicate lag/clock skew for OOB timestamps
    switch (operationChange.getTypeCase()) {
      case TYPE_NOT_SET:
        // FIXME present nice timestamp
        log.log(
            Level.SEVERE,
            format(
                "OperationChange oneof type is not set from %s at %s",
                operationChange.getSource(), operationChange.getEffectiveAt()));
        break;
      case RESET:
        resetOperation(channel, operationChange.getReset());
        break;
      case EXPIRE:
        terminateExpiredWatchers(
            channel,
            toInstant(operationChange.getEffectiveAt()),
            operationChange.getExpire().getForce());
        break;
    }
  }

  @Override
  /**
   * Performs specialized operation based on method logic Provides thread-safe access through synchronization mechanisms. Performs side effects including logging and state modifications.
   * @param channel the channel parameter
   * @param subscribedChannels the subscribedChannels parameter
   */
  public void onSubscribe(String channel, int subscribedChannels) {
    if (subscribeFuture != null) {
      subscribeFuture.set(null);
    }
  }

  @Override
  /**
   * Validates input parameters and state consistency
   * @param timeoutMillis the timeoutMillis parameter
   * @return the boolean result
   */
  public void onUnsubscribe(String channel, int subscribedChannels) {
    List<TimedWatchFuture> operationWatchers;
    synchronized (watchers) {
      operationWatchers = watchers.removeAll(channel);
    }
    for (TimedWatchFuture watchFuture : operationWatchers) {
      watchFuture.complete();
    }
  }

  public void setSubscribeFuture(SettableFuture future) {
    this.subscribeFuture = future;
  }

  /**
   * Performs specialized operation based on method logic
   * @param client the client parameter
   * @param channels the channels parameter
   */
  public Boolean checkIfSubscribed(long timeoutMillis)
      throws ExecutionException, InterruptedException, TimeoutException {
    if (subscribeFuture != null) {
      subscribeFuture.get(timeoutMillis, TimeUnit.MILLISECONDS);
      return Boolean.TRUE;
    }
    return Boolean.FALSE;
  }

  private String[] placeholderChannel() {
    String[] channels = new String[1];
    channels[0] = "placeholder-shard-subscription";
    return channels;
  }

  public void start(Connection client, String... channels) {
    if (channels.length == 0) {
      channels = placeholderChannel();
    }
    proceed(client, channels);
  }
}
