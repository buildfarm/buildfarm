---
layout: default
title: Redis
parent: Architecture
nav_order: 7
---

# Redis

Redis is used as the in-memory database between sharded actors.
Buildfarm's backplane uses a [Jedis Cluster](https://github.com/xetorthio/jedis) for various abstractions.

## Balanced Queues
To balance CPU utilization across multiple nodes in a redis cluster, we distribute operations through redis hashtags.  We have a conceptual queue that uses multiple redis lists in its implementation.
![Balanced Queues]({{site.url}}{{site.baseurl}}/assets/images/BalancedQueues.png)

## Queue Backend Options

By default, balanced queues use Redis Lists. Buildfarm also supports two alternative queue backends:

- **Priority Queues** (`priorityQueue: true`) — Uses Redis Sorted Sets with a Lua script to support operation prioritization via Bazel's `--remote_execution_priority` flag.
- **Stream Queues** (`streamQueue: true`) — Uses [Redis Streams](redis-stream-queues) with consumer groups for built-in pending entry tracking and atomic message acknowledgment. See the [Redis Stream Queues](redis-stream-queues) page for details.