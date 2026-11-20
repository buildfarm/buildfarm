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

## Dispatched operation counts

When an operation is dispatched, its record stores the selected provision queue's
canonical status name, including its Redis hashtag. Lease refreshes preserve that
name, so attribution does not change when queue configuration changes.

`BackplaneStatus.dispatched_operations.from_queues` reports counts grouped by
these names. Both `dispatched_operations.size` and the existing `dispatched_size`
field include every dispatched operation. Records created by older servers can
lack a queue name; those records contribute to the total but not a per-queue
count. Per-queue counts may therefore sum to less than the total during a rolling
upgrade. The existing `dispatched_operations_size` Prometheus gauge continues to
report the total; the per-queue breakdown is exposed through backplane status.

Collecting the breakdown reads the dispatched-operation hash and groups its
records in memory. Its cost grows with the number and size of dispatched records;
it does not fetch actions, commands, queued operations, or CAS blobs. The server's
background metrics collector requests backplane status every 30 seconds. Other
status clients can also trigger this work, so avoid polling it at high frequency
on large installations. Collections taking at least one second produce a warning.
