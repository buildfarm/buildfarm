---
layout: default
title: Metrics
has_children: true
nav_order: 7
---

## Prometheus Configuration

To enable emitting of Prometheus metrics, add the following configuration to your configuration file:

```
server:
  prometheusPort: 9090
```

## Available Prometheus Metrics

**remote_invocations**

Counter for the number of invocations of the capabilities service

**expired_key**

Counter for key expirations

**execution_success**

Counter for the number of successful executions

**pre_queue_size**

Gauge of a number of items in prequeue

**cas_miss**

Counter for number of CAS misses from worker-worker

**queue_failure**

Counter for number of operations that failed to queue

**requeue_failure**

Counter for number of operations that failed to requeue

**dispatched_operations_size**

Gauge of the number of dispatched operations





**worker_pool_size**

Gauge of the number of workers available

**storage_worker_pool_size**

Gauge of the number of storage workers available

**execute_worker_pool_size**

Gauge of the number of execute workers available.

**queue_size**

Gauge of the size of the queue (using a queue_name label for each individual queue)

**actions**

Counter for the number of actions processed

**operations_stage_load**

Counter for the number of operations in each stage (using a `stage_name` for each individual stage)

**operation_status**

Counter for the completed operations status (using a `code` label for each individual [GRPC status code](https://grpc.github.io/grpc/core/md_doc_statuscodes.html))

**operation_exit_code**

Counter for the completed operations exit code (using an `exit_code` label for each individual execution exit code)

**operation_worker**

Counter for the number of operations executed on each worker (using a `worker_name` label for each individual worker)

**action_results**

Counter for the number of action results

**missing_blobs**

Histogram for the number of missing blobs

**execution_slot_usage**

Gauge for the number of execution slots used on each worker

**execution_time_ms**

Histogram for the execution time on a worker (in milliseconds)

**execution_stall_time_ms**

Histogram for the execution stall time on a worker (in milliseconds)

**input_fetch_slot_usage**

Gauge for the number of input fetch slots used on each worker

**input_fetch_time_ms**

Histogram for the input fetch time on a worker (in milliseconds)

**input_fetch_stall_time_ms**

Histogram for the input fetch stall time on a worker (in milliseconds)

**queued_time_ms**

Histogram for the operation queued time (in milliseconds)

**output_upload_time_ms**

Histogram for the output upload time (in milliseconds)

**completed_operations**

Counter for the number of completed operations

**operation_poller**

Counter for the number of operations being polled

**io_bytes**

Histogram for the bytes read/written to get system I/O

**health_check**

Counter showing service restarts

**cas_size**

Gauge of total size of the worker's CAS in bytes

**cas_ttl_s**

Histogram for amount of time CAS entries live on L1 storage before expiration (seconds)

**cas_entry_count**

Gauge of the total number of entries in the worker's CAS

**cas_copy_fallback**
Counter for the number of times the CAS performed a file copy because hardlinking failed

Java interceptors can be used to monitor Grpc services using Prometheus.  To enable [these metrics](https://github.com/grpc-ecosystem/java-grpc-prometheus), add the following configuration to your server:
```
server:
  grpcMetrics:
    enabled: true
    provideLatencyHistograms: false
```
