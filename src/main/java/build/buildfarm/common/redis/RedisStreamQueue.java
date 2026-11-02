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

import static redis.clients.jedis.params.ScanParams.SCAN_POINTER_START;

import build.buildfarm.common.Queue;
import build.buildfarm.common.Visitor;
import com.google.common.collect.ImmutableList;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import redis.clients.jedis.AbstractPipeline;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.params.XAutoClaimParams;
import redis.clients.jedis.params.XPendingParams;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.resps.ScanResult;
import redis.clients.jedis.resps.StreamEntry;
import redis.clients.jedis.resps.StreamPendingEntry;
import redis.clients.jedis.resps.StreamPendingSummary;

/**
 * A Redis queue implementation backed by Redis Streams with consumer groups.
 *
 * <p>This replaces the list-based {@link RedisQueue} with Redis Streams, providing:
 *
 * <ul>
 *   <li>Built-in pending entries list (PEL) replacing manual dequeue shadow lists
 *   <li>Consumer groups for balanced message distribution
 *   <li>XACK for atomic message acknowledgment replacing LREM from dequeue
 *   <li>Message persistence and replay capability
 * </ul>
 *
 * <p>Each stream uses a fixed consumer group ("buildfarm") and a per-JVM consumer name for message
 * delivery. Pending entry IDs are tracked in a static concurrent map for fast lookup during
 * acknowledgment. A Lua-script fallback scans the PEL when the in-memory mapping is unavailable
 * (e.g., after JVM restart).
 */
public class RedisStreamQueue implements Queue<String> {
  private static final Logger log = Logger.getLogger(RedisStreamQueue.class.getName());

  static final String GROUP_NAME = "buildfarm";
  static final String VALUE_FIELD = "v";
  private static final int VISIT_PAGE_SIZE = 1000;

  /** Unique consumer name per JVM to avoid message duplication within a consumer group. */
  private static final String CONSUMER_NAME =
      "buildfarm-" + UUID.randomUUID().toString().substring(0, 8);

  /**
   * In-memory mapping from stream name → (value → stream entry ID) for pending messages. Shared
   * across all RedisStreamQueue instances to support the decorator pattern where new Queue
   * instances are created per operation.
   */
  private static final ConcurrentHashMap<String, ConcurrentHashMap<String, StreamEntryID>>
      pendingEntries = new ConcurrentHashMap<>();

  /**
   * Lua script to find and acknowledge a pending message by its value. Falls back to scanning the
   * PEL when the in-memory entry ID mapping is unavailable.
   */
  private static final String REMOVE_BY_VALUE_SCRIPT =
      String.join(
          "\n",
          "local stream = KEYS[1]",
          "local group = ARGV[1]",
          "local val = ARGV[2]",
          "local pending = redis.call('XPENDING', stream, group, '-', '+', 1000)",
          "for _, entry in ipairs(pending) do",
          "  local id = entry[1]",
          "  local messages = redis.call('XRANGE', stream, id, id)",
          "  if #messages > 0 then",
          "    local fields = messages[1][2]",
          "    for i = 1, #fields, 2 do",
          "      if fields[i] == '" + VALUE_FIELD + "' and fields[i+1] == val then",
          "        redis.call('XACK', stream, group, id)",
          "        redis.call('XDEL', stream, id)",
          "        return 1",
          "      end",
          "    end",
          "  end",
          "end",
          "return 0");

  public static Queue<String> decorate(Jedis jedis, String name) {
    return new RedisStreamQueue(jedis, name);
  }

  private final Jedis jedis;
  private final String name;

  public RedisStreamQueue(Jedis jedis, String name) {
    this.jedis = jedis;
    this.name = name;
  }

  /**
   * Push a value onto the stream.
   *
   * @param val The value to add.
   * @return true always (stream append never fails for valid input).
   */
  @Override
  public boolean offer(String val) {
    return offer(val, 0);
  }

  /**
   * Push a value onto the stream. Priority is ignored for the non-priority stream queue; use {@link
   * RedisPriorityQueue} for priority support.
   *
   * @param val The value to add.
   * @param priority Ignored — present for interface compatibility.
   * @return true always.
   */
  @Override
  public boolean offer(String val, double priority) {
    Map<String, String> fields = new HashMap<>();
    fields.put(VALUE_FIELD, val);
    jedis.xadd(name, XAddParams.xAddParams().id(StreamEntryID.NEW_ENTRY), fields);
    return true;
  }

  /**
   * Blocking dequeue via XREADGROUP. Moves the message to the consumer's pending entries list
   * (PEL). The message stays pending until {@link #removeFromDequeue} acknowledges it.
   *
   * @param timeout How long to block waiting for a message.
   * @return The value, or null if the timeout expired.
   */
  @Override
  public String take(Duration timeout) throws InterruptedException {
    ensureGroupExists();
    int blockMillis = (int) Math.min(timeout.toMillis(), Integer.MAX_VALUE);

    XReadGroupParams params = XReadGroupParams.xReadGroupParams().count(1).block(blockMillis);
    Map<String, StreamEntryID> streams = new HashMap<>();
    streams.put(name, StreamEntryID.UNRECEIVED_ENTRY);

    List<Map.Entry<String, List<StreamEntry>>> result =
        jedis.xreadGroup(GROUP_NAME, CONSUMER_NAME, params, streams);

    if (result == null || result.isEmpty()) {
      return null;
    }

    List<StreamEntry> entries = result.getFirst().getValue();
    if (entries == null || entries.isEmpty()) {
      return null;
    }

    StreamEntry entry = entries.getFirst();
    String value = entry.getFields().get(VALUE_FIELD);
    trackPendingEntry(value, entry.getID());
    return value;
  }

  /**
   * Non-blocking dequeue via XREADGROUP. Same semantics as {@link #take} but returns immediately if
   * no message is available.
   *
   * @return The value, or null if the stream is empty.
   */
  @Override
  public String poll() {
    ensureGroupExists();

    XReadGroupParams params = XReadGroupParams.xReadGroupParams().count(1);
    Map<String, StreamEntryID> streams = new HashMap<>();
    streams.put(name, StreamEntryID.UNRECEIVED_ENTRY);

    List<Map.Entry<String, List<StreamEntry>>> result =
        jedis.xreadGroup(GROUP_NAME, CONSUMER_NAME, params, streams);

    if (result == null || result.isEmpty()) {
      return null;
    }

    List<StreamEntry> entries = result.getFirst().getValue();
    if (entries == null || entries.isEmpty()) {
      return null;
    }

    StreamEntry entry = entries.getFirst();
    String value = entry.getFields().get(VALUE_FIELD);
    trackPendingEntry(value, entry.getID());
    return value;
  }

  /**
   * Acknowledge and delete a message from the stream. Replaces the dequeue shadow list removal
   * pattern. First attempts a fast-path lookup of the stream entry ID from the in-memory map; falls
   * back to a Lua script that scans the PEL by value.
   *
   * @param val The value to acknowledge.
   * @return true if the message was found and acknowledged.
   */
  @Override
  public boolean removeFromDequeue(String val) {
    // Fast path: lookup entry ID from in-memory tracking
    StreamEntryID entryId = removePendingEntry(val);
    if (entryId != null) {
      jedis.xack(name, GROUP_NAME, entryId);
      jedis.xdel(name, entryId);
      return true;
    }

    // Slow path: scan PEL via Lua script
    Object result =
        jedis.eval(
            REMOVE_BY_VALUE_SCRIPT, ImmutableList.of(name), ImmutableList.of(GROUP_NAME, val));
    return result != null && ((Long) result) == 1;
  }

  /**
   * Pipeline-compatible acknowledgment. Uses the in-memory entry ID map for the fast path. Falls
   * back to a Lua eval in the pipeline for recovery.
   *
   * @param pipeline The pipeline to add commands to.
   * @param val The value to acknowledge.
   */
  @Override
  public void removeFromDequeue(AbstractPipeline pipeline, String val) {
    StreamEntryID entryId = removePendingEntry(val);
    if (entryId != null) {
      pipeline.xack(name, GROUP_NAME, entryId);
      pipeline.xdel(name, entryId);
    } else {
      pipeline.eval(
          REMOVE_BY_VALUE_SCRIPT, ImmutableList.of(name), ImmutableList.of(GROUP_NAME, val));
    }
  }

  /**
   * Reclaim stale messages from the Pending Entries List using XAUTOCLAIM. Messages that have been
   * pending longer than {@code minIdleMillis} are re-enqueued as new stream entries, and the old
   * entries are acknowledged and deleted.
   *
   * @param minIdleMillis minimum idle time in milliseconds before a message is reclaimed.
   * @return the number of messages reclaimed.
   */
  @Override
  public int reclaimStaleMessages(long minIdleMillis) {
    ensureGroupExists();
    int reclaimed = 0;
    StreamEntryID startId = new StreamEntryID(0, 0);

    while (true) {
      Map.Entry<StreamEntryID, List<StreamEntry>> result =
          jedis.xautoclaim(
              name,
              GROUP_NAME,
              CONSUMER_NAME,
              minIdleMillis,
              startId,
              new XAutoClaimParams().count(100));

      StreamEntryID nextCursor = result.getKey();
      List<StreamEntry> claimed = result.getValue();

      if (claimed != null) {
        for (StreamEntry entry : claimed) {
          String value = entry.getFields().get(VALUE_FIELD);
          if (value != null) {
            offer(value);
            jedis.xack(name, GROUP_NAME, entry.getID());
            jedis.xdel(name, entry.getID());
            reclaimed++;
          }
        }
      }

      // A cursor of "0-0" means no more stale entries to process
      if (nextCursor == null
          || (nextCursor.getTime() == 0 && nextCursor.getSequence() == 0)
          || claimed == null
          || claimed.isEmpty()) {
        break;
      }
      startId = nextCursor;
    }

    return reclaimed;
  }

  /**
   * Get the dequeue name for compatibility. Stream queues don't have a separate dequeue key; the
   * PEL serves this purpose. Returns a conventional name for logging and diagnostics.
   */
  public String getDequeueName() {
    return name + "_pending";
  }

  /**
   * Get the number of undelivered messages in the stream. Subtracts pending (in-flight) entries
   * from the total stream length.
   */
  @Override
  public long size() {
    long total = jedis.xlen(name);
    try {
      StreamPendingSummary summary = jedis.xpending(name, GROUP_NAME);
      return total - summary.getTotal();
    } catch (Exception e) {
      // Group might not exist yet — all entries are undelivered
      return total;
    }
  }

  @Override
  public Supplier<Long> size(AbstractPipeline pipeline) {
    // Approximation: XLEN includes pending entries. Accurate count requires XPENDING
    // which isn't easily composed in a pipeline. This matches how the pipeline-based
    // size is used (status reporting, capacity checks where approximation is acceptable).
    return pipeline.xlen(name);
  }

  /**
   * Visit each undelivered message in the stream. Iterates all entries via XRANGE, which includes
   * both delivered (pending) and undelivered entries. This matches the semantics of the list-based
   * visit which iterates the main queue.
   */
  @Override
  public void visit(Visitor<String> visitor) {
    StreamEntryID start = new StreamEntryID(0, 0);
    while (true) {
      List<StreamEntry> entries = jedis.xrange(name, start, (StreamEntryID) null, VISIT_PAGE_SIZE);
      if (entries == null || entries.isEmpty()) {
        break;
      }
      for (StreamEntry entry : entries) {
        String value = entry.getFields().get(VALUE_FIELD);
        if (value != null) {
          visitor.visit(value);
        }
      }
      if (entries.size() < VISIT_PAGE_SIZE) {
        break;
      }
      // Advance past the last entry for the next page
      StreamEntryID lastId = entries.get(entries.size() - 1).getID();
      start = nextEntryId(lastId);
    }
  }

  /**
   * Visit each pending (in-flight) message. These are messages that have been delivered to a
   * consumer via XREADGROUP but not yet acknowledged via XACK. Replaces visiting the dequeue shadow
   * list.
   */
  @Override
  public void visitDequeue(Visitor<String> visitor) {
    try {
      XPendingParams params = XPendingParams.xPendingParams().count(VISIT_PAGE_SIZE);
      List<StreamPendingEntry> pending = jedis.xpending(name, GROUP_NAME, params);
      if (pending == null || pending.isEmpty()) {
        return;
      }
      for (StreamPendingEntry pe : pending) {
        StreamEntryID id = pe.getID();
        List<StreamEntry> entries = jedis.xrange(name, id, id, 1);
        if (entries != null && !entries.isEmpty()) {
          String value = entries.getFirst().getFields().get(VALUE_FIELD);
          if (value != null) {
            visitor.visit(value);
          }
        }
      }
    } catch (Exception e) {
      // Group might not exist yet — no pending entries
      log.log(Level.FINE, "visitDequeue: no pending entries or group not found", e);
    }
  }

  /**
   * Scan stream entries with cursor-based pagination. The cursor is a stream entry ID string. An
   * empty cursor or "0" starts from the beginning.
   */
  @Override
  public ScanResult<String> scan(String cursor, int count, String match) {
    StreamEntryID start;
    if (cursor == null || cursor.isEmpty() || cursor.equals(SCAN_POINTER_START)) {
      start = new StreamEntryID(0, 0);
    } else {
      start = new StreamEntryID(cursor);
    }

    List<StreamEntry> entries = jedis.xrange(name, start, (StreamEntryID) null, count);
    if (entries == null || entries.isEmpty()) {
      return new ScanResult<>(SCAN_POINTER_START, ImmutableList.of());
    }

    ImmutableList.Builder<String> values = ImmutableList.builder();
    StreamEntryID lastId = null;
    for (StreamEntry entry : entries) {
      String value = entry.getFields().get(VALUE_FIELD);
      if (value != null) {
        values.add(value);
      }
      lastId = entry.getID();
    }

    String nextCursor;
    if (entries.size() < count || lastId == null) {
      nextCursor = SCAN_POINTER_START;
    } else {
      nextCursor = nextEntryId(lastId).toString();
    }

    return new ScanResult<>(nextCursor, values.build());
  }

  /** Ensure the consumer group exists, creating it if necessary. */
  private void ensureGroupExists() {
    try {
      jedis.xgroupCreate(name, GROUP_NAME, StreamEntryID.LAST_ENTRY, true);
    } catch (redis.clients.jedis.exceptions.JedisDataException e) {
      // BUSYGROUP: consumer group already exists — expected in normal operation
      if (!e.getMessage().contains("BUSYGROUP")) {
        throw e;
      }
    }
  }

  /** Track a pending entry ID in the static map for fast removeFromDequeue lookup. */
  private void trackPendingEntry(String value, StreamEntryID entryId) {
    pendingEntries.computeIfAbsent(name, k -> new ConcurrentHashMap<>()).put(value, entryId);
  }

  /**
   * Remove and return the entry ID for a pending value. Returns null if not found in the in-memory
   * map.
   */
  private StreamEntryID removePendingEntry(String value) {
    ConcurrentHashMap<String, StreamEntryID> entries = pendingEntries.get(name);
    if (entries != null) {
      return entries.remove(value);
    }
    return null;
  }

  /** Compute the next stream entry ID after the given one (for pagination). */
  private static StreamEntryID nextEntryId(StreamEntryID id) {
    return new StreamEntryID(id.getTime(), id.getSequence() + 1);
  }

  /** Clear all tracked pending entries for testing purposes. */
  static void clearPendingEntries() {
    pendingEntries.clear();
  }
}
