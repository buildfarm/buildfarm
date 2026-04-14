---
layout: default
title: Redis Stream Queues
parent: Architecture
nav_order: 8
---

# Redis Stream Queues

Buildfarm supports an alternative queue implementation backed by [Redis Streams](https://redis.io/docs/data-types/streams/) with consumer groups. This replaces the default list-based queues with Redis's native stream data structure, eliminating several hand-rolled reliability mechanisms.

## Motivation

The default queue implementation uses Redis Lists with a "dequeue shadow list" pattern for crash recovery. Each queue maintains a second list (suffixed `_dequeue`) to track in-flight messages, and a `DispatchedMonitor` background thread polls for stale operations to requeue them. This works but has drawbacks:

- **Shadow dequeue lists** require manual cleanup and can accumulate stale entries
- **Round-robin polling** across balanced queues uses exponential backoff with busy-waiting
- **Semi-atomic pipelines** make the take-and-track operation non-atomic
- **DispatchedMonitor** adds a polling interval (default 10s) before stale operations are detected

Redis Streams addresses these issues with built-in primitives.

## How It Works

### Queue Operations Mapping

| Operation | List-Based (default) | Stream-Based |
|-----------|---------------------|--------------|
| **Enqueue** | `LPUSH` to a Redis list | `XADD` to a Redis stream |
| **Blocking dequeue** | `BLMOVE` from queue list to dequeue list | `XREADGROUP` — message auto-tracked in Pending Entries List (PEL) |
| **Non-blocking dequeue** | `LMOVE` from queue list to dequeue list | `XREADGROUP` without `BLOCK` |
| **Acknowledge completion** | `LREM` from dequeue list | `XACK` + `XDEL` — single atomic acknowledgment |
| **Queue size** | `LLEN` of queue list | `XLEN` minus `XPENDING` count |
| **Crash recovery** | DispatchedMonitor scans dispatched hash | Messages stay in PEL until acknowledged; recoverable via `XAUTOCLAIM` |

### Consumer Groups

Stream queues use a fixed consumer group named `buildfarm`. All server and worker instances that read from a queue join this group. Redis guarantees that each message is delivered to exactly one consumer within the group, providing automatic load balancing without application-level round-robin.

Each JVM instance uses a unique consumer name (generated at startup) so that Redis can track which messages are pending per consumer.

### Pending Entries List (PEL)

When a consumer reads a message via `XREADGROUP`, Redis moves the message into the consumer's Pending Entries List. The message stays pending until explicitly acknowledged with `XACK`. This replaces the dequeue shadow list pattern:

- No separate `_dequeue` keys in Redis
- No `LREM` scans to find and remove values
- The PEL is maintained atomically by Redis
- Pending messages survive consumer crashes

### Entry ID Tracking

Stream messages have unique entry IDs (e.g., `1709136000000-0`) assigned by Redis. Since the `Queue<String>` interface identifies messages by value (not ID), the stream queue maintains an in-memory mapping from message values to their stream entry IDs. This allows `removeFromDequeue(value)` to translate the value into the correct entry ID for `XACK`.

If the in-memory mapping is unavailable (e.g., after a JVM restart), a Lua script fallback scans the PEL to find the message by value. This is O(n) in the number of pending entries, which is typically small (bounded by the number of concurrent operations per queue shard).

## Configuration

Enable stream queues by setting `streamQueue: true` in the backplane configuration:

```yaml
backplane:
  type: SHARD
  redisUri: "redis://localhost:6379"
  streamQueue: true
  queues:
    - name: "cpu"
      allowUnmatched: true
      properties:
        - name: "min-cores"
          value: "*"
        - name: "max-cores"
          value: "*"
```

### Configuration Options

| Parameter | Default | Description |
|-----------|---------|-------------|
| `streamQueue` | `false` | Use Redis Streams instead of Redis Lists for all operation queues (prequeue and execution queues) |
| `streamPelMonitorIntervalSeconds` | `30` | How often (seconds) the StreamPelMonitor checks for stale PEL entries |
| `streamPelMinIdleMillis` | `60000` | Minimum idle time (ms) before a pending entry is reclaimed via XAUTOCLAIM |

The `streamQueue` setting applies to both the prequeue (arrival queue) and all provisioned execution queues. Queue names, provision matching, and balanced distribution across Redis cluster nodes all work identically to list-based queues.

### Interaction with `priorityQueue`

The `streamQueue` and `priorityQueue` settings are alternatives:

| `streamQueue` | `priorityQueue` | Queue Implementation |
|---|---|---|
| `false` | `false` | `RedisQueue` — FIFO list (default) |
| `false` | `true` | `RedisPriorityQueue` — sorted set with Lua script |
| `true` | `false` | `RedisStreamQueue` — Redis Stream with consumer groups |
| `true` | `true` | `RedisStreamQueue` — stream takes precedence (no priority ordering) |

Stream queues do not currently support priority ordering. If both flags are set, `streamQueue` takes precedence and operations are processed in arrival order (FIFO). Priority support for stream queues is a planned enhancement using multiple streams per priority level.

## Requirements

- **Redis 5.0+** — Streams and consumer groups were introduced in Redis 5.0. The `XAUTOCLAIM` command (used for stale message recovery) requires Redis 6.2+.
- **Jedis 5.x** — Buildfarm's Jedis dependency already supports stream operations.

## Migration

### From List-Based Queues

Stream queues use different Redis keys than list-based queues (stream keys with `_stream` suffix vs. list keys). Switching between modes requires draining the existing queues first:

1. Stop all schedulers and workers
2. Wait for in-flight operations to complete or expire
3. Update the configuration to `streamQueue: true`
4. Restart schedulers and workers

Operations that were in the list-based queues will not be automatically migrated. Any operations in the dispatched state will be handled by the DispatchedMonitor as usual.

### Rollback

To revert to list-based queues, follow the same drain procedure in reverse. Set `streamQueue: false` and restart all components.

## Observability

Stream queues report the same status information as list-based queues through the existing OperationQueue gRPC service. Queue sizes, per-shard distribution, and provisioned queue breakdowns all work identically.

Pending entries (equivalent to the dequeue list) can be inspected directly in Redis:

```bash
# List pending entries for a stream
redis-cli XPENDING "{06S}{Execution}:QueuedOperations_stream" buildfarm - + 10

# Get stream length
redis-cli XLEN "{06S}{Execution}:QueuedOperations_stream"

# List consumer group info
redis-cli XINFO GROUPS "{06S}{Execution}:QueuedOperations_stream"

# List consumers in the group
redis-cli XINFO CONSUMERS "{06S}{Execution}:QueuedOperations_stream" buildfarm
```

## PEL Recovery via XAUTOCLAIM

When a server crashes between `XREADGROUP` (which places a message into the PEL) and `XACK` (which acknowledges it), the message becomes orphaned in the Pending Entries List. The `DispatchedMonitor` cannot recover these messages because they never made it into the `dispatchedExecutions` hash.

The `StreamPelMonitor` addresses this gap. It runs as a background thread on each server instance and periodically uses the Redis `XAUTOCLAIM` command (Redis 6.2+) to find and reclaim stale PEL entries:

1. **Scan**: `XAUTOCLAIM` finds messages in the PEL that have been idle longer than `streamPelMinIdleMillis`
2. **Re-enqueue**: Each stale message is re-added to the stream as a new entry via `XADD`
3. **Clean up**: The old PEL entry is acknowledged (`XACK`) and deleted (`XDEL`)

This is **complementary** to `DispatchedMonitor` — they address different failure windows:

| Monitor | Failure Window | Recovery Mechanism |
|---------|---------------|-------------------|
| `DispatchedMonitor` | Worker crashes after dispatch | Scans `dispatchedExecutions` hash, requeues overdue operations |
| `StreamPelMonitor` | Server crashes between XREADGROUP and XACK | Scans stream PEL via XAUTOCLAIM, re-enqueues stale entries |

The monitor uses cursor-based pagination to efficiently process large PELs without blocking, and logs the number of reclaimed entries at each interval.

## Limitations

- **No priority ordering** — Stream entries are ordered by insertion time. Priority queues (`priorityQueue: true`) should continue using the sorted-set-based `RedisPriorityQueue` until stream-based priority support is added.
- **Higher memory usage** — Redis Streams use more memory per entry than Lists due to the radix tree structure and per-entry metadata. For typical Buildfarm queue depths (10K–100K entries), this difference is negligible.
- **Consumer accumulation** — Each JVM instance creates a consumer in the group. Consumers persist until explicitly deleted. In environments with frequent container restarts, stale consumers may accumulate. These can be cleaned up with `XGROUP DELCONSUMER`.
