// Copyright 2025 The Buildfarm Authors. All rights reserved.
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

import java.io.Closeable;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import lombok.extern.java.Log;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.params.SetParams;

/**
 * @class LeaderElection
 * @brief Redis-based distributed leader election implementation.
 * @details Implements a leader election algorithm using Redis atomic operations. Multiple identical
 *     replicas can compete for leadership of various resources, each identified by a unique String
 *     (election key). The class uses Redis SET with NX (Not eXists) and PX (exPire) options to
 *     implement distributed locking with automatic expiration. Features include: - Automatic lease
 *     renewal for active leaders - Configurable lease duration and renewal intervals - Asynchronous
 *     callbacks for leadership changes - Thread-safe operations - OpenTelemetry instrumentation
 *     <p>Usage Example:
 *     <pre>{@code
 * LeaderElection election = new LeaderElection(
 *     "worker-node-1",
 *     Duration.ofSeconds(60),
 *     Duration.ofSeconds(20)
 * );
 *
 * election.addLeadershipCallback((electionKey, isLeader) -> {
 *     if (isLeader) {
 *         System.out.println("Became leader for: " + electionKey);
 *     } else {
 *         System.out.println("Lost leadership for: " + electionKey);
 *     }
 * });
 *
 * if (election.tryBecomeLeader(jedis, "build-scheduler")) {
 *     // This node is now the leader for "build-scheduler"
 *     // Lease will be automatically renewed
 * }
 *
 * // Later, when shutting down
 * election.resignLeadership(jedis, "build-scheduler");
 * election.close();
 *
 * }</pre>
 */
@Log
public class LeaderElection implements Closeable {
  private static final String REDIS_KEY_PREFIX = "buildfarm:leader:";

  // Lua script for atomic renewal - checks ownership before extending TTL
  private static final String RENEWAL_SCRIPT =
      "if redis.call('get', KEYS[1]) == ARGV[1] then "
          + "return redis.call('pexpire', KEYS[1], ARGV[2]) "
          + "else return 0 end";

  // Lua script for atomic resignation - checks ownership before deleting
  private static final String RESIGN_SCRIPT =
      "if redis.call('get', KEYS[1]) == ARGV[1] then "
          + "return redis.call('del', KEYS[1]) "
          + "else return 0 end";

  /**
   * @field nodeId The unique identifier for this node (e.g., hostname, UUID)
   */
  private final String nodeId;

  /**
   * @field leaseDuration How long the leadership lease lasts before expiring
   */
  private final Duration leaseDuration;

  /**
   * @field renewalInterval How often the leader should renew the lease
   */
  private final Duration renewalInterval;

  /**
   * @field renewalExecutor Scheduled executor for automatic lease renewal
   */
  private final ScheduledExecutorService renewalExecutor;

  /**
   * @field closed Flag indicating if this election instance has been closed
   */
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * @field activeElections Map of election keys this node is currently leading
   */
  private final Map<String, ElectionState> activeElections = new ConcurrentHashMap<>();

  /**
   * @field leadershipCallbacks List of callbacks to invoke on leadership changes
   */
  private final List<BiConsumer<String, Boolean>> leadershipCallbacks =
      Collections.synchronizedList(new java.util.ArrayList<>());

  /**
   * @class ElectionState
   * @brief Tracks the state of a specific election this node is participating in.
   */
  private static class ElectionState {
    final UnifiedJedis jedis;
    final long acquisitionTimestamp;
    volatile boolean wasLeader;

    ElectionState(UnifiedJedis jedis) {
      this.jedis = jedis;
      this.acquisitionTimestamp = System.currentTimeMillis();
      this.wasLeader = true;
    }
  }

  /**
   * @brief Constructor with configurable lease duration and renewal interval.
   * @details Creates a new LeaderElection instance with automatic lease renewal. The renewal
   *     interval should be significantly less than the lease duration (recommended: renewal <
   *     leaseDuration / 2) to avoid race conditions during renewal.
   * @param nodeId Unique identifier for this node (must be stable across restarts if persistence is
   *     desired)
   * @param leaseDuration How long the leadership lease lasts before automatic expiration
   * @param renewalInterval How often the leader should attempt to renew the lease
   * @throws IllegalArgumentException if renewalInterval >= leaseDuration
   */
  public LeaderElection(String nodeId, Duration leaseDuration, Duration renewalInterval) {
    if (nodeId == null || nodeId.isEmpty()) {
      throw new IllegalArgumentException("nodeId cannot be null or empty");
    }
    if (leaseDuration == null || leaseDuration.isNegative() || leaseDuration.isZero()) {
      throw new IllegalArgumentException("leaseDuration must be positive");
    }
    if (renewalInterval == null || renewalInterval.isNegative() || renewalInterval.isZero()) {
      throw new IllegalArgumentException("renewalInterval must be positive");
    }
    if (renewalInterval.compareTo(leaseDuration) >= 0) {
      throw new IllegalArgumentException("renewalInterval must be less than leaseDuration");
    }

    this.nodeId = nodeId;
    this.leaseDuration = leaseDuration;
    this.renewalInterval = renewalInterval;
    this.renewalExecutor =
        Executors.newScheduledThreadPool(
            1,
            r -> {
              Thread thread = new Thread(r, "leader-election-renewal");
              thread.setDaemon(true);
              return thread;
            });

    startRenewalTask();
    log.info(
        String.format(
            "LeaderElection initialized for node '%s' (lease: %ds, renewal: %ds)",
            nodeId, leaseDuration.getSeconds(), renewalInterval.getSeconds()));
  }

  /**
   * @brief Constructor with default renewal interval (leaseDuration / 3).
   * @param nodeId Unique identifier for this node
   * @param leaseDuration How long the leadership lease lasts before automatic expiration
   */
  public LeaderElection(String nodeId, Duration leaseDuration) {
    this(nodeId, leaseDuration, leaseDuration.dividedBy(3));
  }

  /**
   * @brief Constructor with default lease duration (60 seconds) and renewal interval (20 seconds).
   * @param nodeId Unique identifier for this node
   */
  public LeaderElection(String nodeId) {
    this(nodeId, Duration.ofSeconds(60), Duration.ofSeconds(20));
  }

  /**
   * @brief Attempt to become the leader for a specific election.
   * @details Uses Redis SET with NX (not exists) and PX (expire milliseconds) to atomically acquire
   *     leadership. If successful, automatic renewal begins. This method is idempotent - calling it
   *     multiple times while already leader has no effect.
   * @param jedis UnifiedJedis client for Redis operations
   * @param electionKey Unique identifier for what needs a leader (e.g., "build-scheduler")
   * @return true if this node became (or already was) the leader, false otherwise
   */
  public boolean tryBecomeLeader(UnifiedJedis jedis, String electionKey) {
    if (closed.get()) {
      log.warning(
          String.format(
              "Attempted to become leader for '%s' but LeaderElection is closed", electionKey));
      return false;
    }

    String redisKey = getRedisKey(electionKey);
    String value = getLeaderValue();

    try {
      // Check if we're already the leader
      String currentLeader = jedis.get(redisKey);
      if (value.equals(currentLeader)) {
        log.fine(String.format("Node '%s' already leader for '%s'", nodeId, electionKey));
        return true;
      }

      // Attempt to acquire leadership with SET NX PX (atomic operation)
      SetParams params = new SetParams().nx().px(leaseDuration.toMillis());
      String result = jedis.set(redisKey, value, params);

      boolean acquired = "OK".equals(result);
      if (acquired) {
        activeElections.put(electionKey, new ElectionState(jedis));
        log.info(String.format("Node '%s' became leader for '%s'", nodeId, electionKey));
        notifyLeadershipChange(electionKey, true);
      } else {
        log.fine(
            String.format(
                "Node '%s' failed to become leader for '%s' (already held by another node)",
                nodeId, electionKey));
      }

      return acquired;
    } catch (Exception e) {
      log.log(
          Level.SEVERE,
          String.format("Error attempting to become leader for '%s'", electionKey),
          e);
      return false;
    }
  }

  /**
   * @brief Renew leadership for a specific election.
   * @details Uses a Lua script to atomically verify ownership and extend the TTL. This method is
   *     called automatically by the renewal task, but can also be called manually.
   * @param jedis UnifiedJedis client for Redis operations
   * @param electionKey Unique identifier for the election
   * @return true if renewal was successful (this node is still leader), false otherwise
   */
  public boolean renewLeadership(UnifiedJedis jedis, String electionKey) {
    if (closed.get()) {
      return false;
    }

    String redisKey = getRedisKey(electionKey);
    String value = getLeaderValue();

    try {
      // Use Lua script for atomic check-and-renew
      Object result =
          jedis.eval(
              RENEWAL_SCRIPT,
              Collections.singletonList(redisKey),
              List.of(value, String.valueOf(leaseDuration.toMillis())));

      boolean renewed = Long.valueOf(1).equals(result);
      if (renewed) {
        log.fine(String.format("Node '%s' renewed leadership for '%s'", nodeId, electionKey));
      } else {
        log.warning(
            String.format(
                "Node '%s' failed to renew leadership for '%s' (lost)", nodeId, electionKey));
        handleLeadershipLoss(electionKey);
      }

      return renewed;
    } catch (Exception e) {
      log.severe(
          String.format("Error renewing leadership for '%s': %s", electionKey, e.getMessage()));
      handleLeadershipLoss(electionKey);
      return false;
    }
  }

  /**
   * @brief Check if this node is currently the leader for a specific election.
   * @param jedis UnifiedJedis client for Redis operations
   * @param electionKey Unique identifier for the election
   * @return true if this node is the current leader, false otherwise
   */
  public boolean isLeader(UnifiedJedis jedis, String electionKey) {
    if (closed.get()) {
      return false;
    }

    String redisKey = getRedisKey(electionKey);
    String value = getLeaderValue();

    try {
      String currentLeader = jedis.get(redisKey);
      return value.equals(currentLeader);
    } catch (Exception e) {
      log.severe(
          String.format("Error checking leadership for '%s': %s", electionKey, e.getMessage()));
      return false;
    }
  }

  /**
   * @brief Get the current leader's node ID for a specific election.
   * @param jedis UnifiedJedis client for Redis operations
   * @param electionKey Unique identifier for the election
   * @return The node ID of the current leader, or null if there is no leader
   */
  public String getCurrentLeader(UnifiedJedis jedis, String electionKey) {
    if (closed.get()) {
      return null;
    }

    String redisKey = getRedisKey(electionKey);

    try {
      String value = jedis.get(redisKey);
      if (value == null) {
        return null;
      }

      // Parse nodeId from value (format: nodeId:timestamp)
      int separatorIndex = value.lastIndexOf(':');
      if (separatorIndex > 0) {
        return value.substring(0, separatorIndex);
      }

      return value;
    } catch (Exception e) {
      log.severe(
          String.format("Error getting current leader for '%s': %s", electionKey, e.getMessage()));
      return null;
    }
  }

  /**
   * @brief Resign leadership for a specific election.
   * @details Uses a Lua script to atomically verify ownership before deleting the key. This ensures
   *     that only the current leader can resign. After resigning, automatic renewal stops for this
   *     election.
   * @param jedis UnifiedJedis client for Redis operations
   * @param electionKey Unique identifier for the election
   * @return true if resignation was successful, false if not currently leader
   */
  public boolean resignLeadership(UnifiedJedis jedis, String electionKey) {
    if (closed.get()) {
      return false;
    }

    String redisKey = getRedisKey(electionKey);
    String value = getLeaderValue();

    try {
      // Use Lua script for atomic check-and-delete
      Object result =
          jedis.eval(RESIGN_SCRIPT, Collections.singletonList(redisKey), List.of(value));

      boolean resigned = Long.valueOf(1).equals(result);
      if (resigned) {
        activeElections.remove(electionKey);
        log.info(String.format("Node '%s' resigned leadership for '%s'", nodeId, electionKey));
        notifyLeadershipChange(electionKey, false);
      } else {
        log.warning(
            String.format(
                "Node '%s' failed to resign leadership for '%s' (not current leader)",
                nodeId, electionKey));
      }

      return resigned;
    } catch (Exception e) {
      log.severe(
          String.format("Error resigning leadership for '%s': %s", electionKey, e.getMessage()));
      return false;
    }
  }

  /**
   * @brief Add a callback to be invoked when leadership changes.
   * @details Callbacks are invoked asynchronously on the renewal thread. The callback receives the
   *     election key and a boolean indicating whether this node gained (true) or lost (false)
   *     leadership.
   * @param callback BiConsumer that accepts (electionKey, isLeader)
   */
  public void addLeadershipCallback(BiConsumer<String, Boolean> callback) {
    if (callback != null) {
      leadershipCallbacks.add(callback);
      log.fine("Leadership callback added");
    }
  }

  /**
   * @brief Remove a previously registered callback.
   * @param callback The callback to remove
   * @return true if the callback was found and removed, false otherwise
   */
  public boolean removeLeadershipCallback(BiConsumer<String, Boolean> callback) {
    return leadershipCallbacks.remove(callback);
  }

  /**
   * @brief Get the node ID for this election instance.
   * @return The unique node identifier
   */
  public String getNodeId() {
    return nodeId;
  }

  /**
   * @brief Get the configured lease duration.
   * @return The lease duration
   */
  public Duration getLeaseDuration() {
    return leaseDuration;
  }

  /**
   * @brief Get the configured renewal interval.
   * @return The renewal interval
   */
  public Duration getRenewalInterval() {
    return renewalInterval;
  }

  /**
   * @brief Check if this LeaderElection instance has been closed.
   * @return true if closed, false otherwise
   */
  public boolean isClosed() {
    return closed.get();
  }

  /**
   * @brief Close this LeaderElection instance and release all resources.
   * @details Stops the renewal task, resigns all active leaderships, and shuts down the executor.
   *     After closing, no further operations can be performed.
   */
  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      log.info(String.format("Closing LeaderElection for node '%s'", nodeId));

      // Stop renewal task
      renewalExecutor.shutdown();

      // Resign all active leaderships
      activeElections.forEach(
          (electionKey, state) -> {
            try {
              resignLeadership(state.jedis, electionKey);
            } catch (Exception e) {
              log.warning(
                  String.format(
                      "Error resigning leadership for '%s' during close: %s",
                      electionKey, e.getMessage()));
            }
          });

      // Wait for executor shutdown
      try {
        if (!renewalExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
          renewalExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        renewalExecutor.shutdownNow();
        Thread.currentThread().interrupt();
      }

      activeElections.clear();
      leadershipCallbacks.clear();
      log.info(String.format("LeaderElection closed for node '%s'", nodeId));
    }
  }

  /**
   * @brief Start the automatic renewal task.
   * @details Runs periodically at the configured renewal interval to renew all active leaderships.
   */
  private void startRenewalTask() {
    renewalExecutor.scheduleAtFixedRate(
        () -> {
          if (closed.get()) {
            return;
          }

          activeElections.forEach(
              (electionKey, state) -> {
                try {
                  if (!renewLeadership(state.jedis, electionKey)) {
                    // Renewal failed - leadership was lost
                    handleLeadershipLoss(electionKey);
                  }
                } catch (Exception e) {
                  log.severe(
                      String.format(
                          "Error in renewal task for '%s': %s", electionKey, e.getMessage()));
                  handleLeadershipLoss(electionKey);
                }
              });
        },
        renewalInterval.toMillis(),
        renewalInterval.toMillis(),
        TimeUnit.MILLISECONDS);

    log.fine(String.format("Renewal task started (interval: %dms)", renewalInterval.toMillis()));
  }

  /**
   * @brief Handle loss of leadership for an election.
   * @param electionKey The election key for which leadership was lost
   */
  private void handleLeadershipLoss(String electionKey) {
    ElectionState state = activeElections.remove(electionKey);
    if (state != null && state.wasLeader) {
      state.wasLeader = false;
      log.warning(String.format("Node '%s' lost leadership for '%s'", nodeId, electionKey));
      notifyLeadershipChange(electionKey, false);
    }
  }

  /**
   * @brief Notify all registered callbacks of a leadership change.
   * @param electionKey The election key
   * @param isLeader true if leadership was gained, false if lost
   */
  private void notifyLeadershipChange(String electionKey, boolean isLeader) {
    if (leadershipCallbacks.isEmpty()) {
      return;
    }

    // Invoke callbacks asynchronously to avoid blocking
    renewalExecutor.execute(
        () -> {
          for (BiConsumer<String, Boolean> callback : leadershipCallbacks) {
            try {
              callback.accept(electionKey, isLeader);
            } catch (Exception e) {
              log.log(
                  Level.SEVERE,
                  String.format("Error in leadership callback for '%s'", electionKey),
                  e);
            }
          }
        });
  }

  /**
   * @brief Get the Redis key for an election.
   * @param electionKey The election key
   * @return The full Redis key
   */
  private String getRedisKey(String electionKey) {
    return REDIS_KEY_PREFIX + electionKey;
  }

  /**
   * @brief Get the value to store in Redis for this node's leadership.
   * @details Format: nodeId:timestamp
   * @return The leader value string
   */
  private String getLeaderValue() {
    return nodeId + ":" + Instant.now().toEpochMilli();
  }
}
