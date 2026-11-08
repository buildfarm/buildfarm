---
layout: default
title: Metrics
has_children: true
nav_order: 7
---

## Prometheus Configuration

To enable emitting Prometheus metrics on the server or worker, add the `prometheusPort` configuration to the top level of your configuration file (or provide `--prometheus_port=<port>` via CLI flags):

```yaml
prometheusPort: 9090
```

When enabled (port > 0), an embedded Prometheus HTTP server exports metrics at `http://<host>:<prometheusPort>/metrics`. In addition to Buildfarm-specific metrics, standard JVM/Hotspot metrics (memory pools, garbage collection, threads, class loading) are exported automatically.

### gRPC Service Metrics

Java interceptors can monitor gRPC services using Prometheus. To enable [these metrics](https://github.com/grpc-ecosystem/java-grpc-prometheus), add the `grpcMetrics` block under `server` or `worker`:

```yaml
server:
  grpcMetrics:
    enabled: true
    provideLatencyHistograms: true
    latencyBuckets: [0.001, 0.01, 0.1, 1, 5, 10, 20, 40, 60, +Infinity]
    labelsToReport: []

worker:
  grpcMetrics:
    enabled: true
    provideLatencyHistograms: false
```

## Available Prometheus Metrics

### Server and Execution Metrics

**remote_invocations**
- **Component**: `buildfarm-server`
- **Type**: Counter
- **Description**: Number of invocations of the Capabilities service (`getCapabilities`).

**execution_success**
- **Component**: `buildfarm-server`
- **Type**: Counter
- **Description**: Number of execution requests successfully accepted and processed.

**merged_executions**
- **Component**: `buildfarm-server`
- **Type**: Counter
- **Description**: Number of duplicate execution requests merged into an already-running action execution.

**pre_queue_size**
- **Component**: `buildfarm-server`
- **Type**: Gauge
- **Description**: Current number of operations waiting in the pre-queue before dispatching into execution queues.

**queue_size**
- **Component**: `buildfarm-server`
- **Type**: Gauge
- **Labels**: `queue_name`
- **Description**: Size of each execution queue on the backplane, broken down by queue name.

**dispatched_operations_size**
- **Component**: `buildfarm-server`
- **Type**: Gauge
- **Description**: Number of dispatched operations currently tracked across the cluster.

**queue_failure**
- **Component**: `buildfarm-server`
- **Type**: Counter
- **Description**: Number of operations that failed to be enqueued to the backplane.

**requeue_failure**
- **Component**: `buildfarm-server`
- **Type**: Counter
- **Description**: Number of operations that failed during a requeue attempt.

**worker_pool_size**
- **Component**: `buildfarm-server`
- **Type**: Gauge
- **Description**: Total number of active workers currently registered on the backplane.

**storage_worker_pool_size**
- **Component**: `buildfarm-server`
- **Type**: Gauge
- **Description**: Total number of active storage-capable workers available.

**execute_worker_pool_size**
- **Component**: `buildfarm-server`
- **Type**: Gauge
- **Description**: Total number of active execution-capable workers available.

**health_check**
- **Component**: `buildfarm-server`, `buildfarm-worker`
- **Type**: Counter
- **Labels**: `lifecycle` (`start`, `stop`)
- **Description**: Tracks service lifecycle events on the server and worker instances.

### Operation Lifecycle and Telemetry Metrics

**actions**
- **Component**: `buildfarm-server`
- **Type**: Counter
- **Description**: Total number of actions processed by the metrics publisher.

**operations_stage_load**
- **Component**: `buildfarm-server`
- **Type**: Counter
- **Labels**: `stage_name` (e.g., `UNKNOWN`, `CACHE_CHECK`, `QUEUED`, `EXECUTING`, `COMPLETED`)
- **Description**: Number of operations entering each execution stage.

**operation_status**
- **Component**: `buildfarm-server`
- **Type**: Counter
- **Labels**: `code` (e.g., `OK`, `CANCELLED`, `DEADLINE_EXCEEDED`, and other [gRPC status codes](https://grpc.github.io/grpc/core/md_doc_statuscodes.html))
- **Description**: Completed operation counts categorized by gRPC return status code.

**operation_exit_code**
- **Component**: `buildfarm-server`
- **Type**: Counter
- **Labels**: `exit_code`
- **Description**: Completed action executions categorized by their process exit code (e.g., `0`, `1`).

**operation_worker**
- **Component**: `buildfarm-server`
- **Type**: Counter
- **Labels**: `worker_name`
- **Description**: Number of operations executed by each worker.

**queued_time_s**
- **Component**: `buildfarm-server`
- **Type**: Histogram
- **Unit**: Seconds
- **Description**: Duration operations spend waiting in the queue (from queued timestamp to worker start timestamp).

**output_upload_time_s**
- **Component**: `buildfarm-server`
- **Type**: Histogram
- **Unit**: Seconds
- **Description**: Duration spent uploading action outputs (from output upload start to completion).

### Action Cache Metrics

**action_results**
- **Component**: `buildfarm-server`
- **Type**: Counter
- **Description**: Total number of `getActionResult` requests received by the Action Cache service.

**action_result_kind**
- **Component**: `buildfarm-server`
- **Type**: Counter
- **Labels**: `kind` (`hit`, `miss`, or gRPC status code in lowercase such as `not_found`)
- **Description**: Result kind returned for `getActionResult` requests.

**action_results_cancelled**
- **Component**: `buildfarm-server`
- **Type**: Counter
- **Description**: Number of `getActionResult` requests cancelled by the client.

### CAS (Content Addressable Storage) Metrics

**missing_blobs**
- **Component**: `buildfarm-server`, `buildfarm-worker`
- **Type**: Histogram
- **Description**: Number of blob digests requested per `findMissingBlobs` call.

**cas_hit**
- **Component**: `buildfarm-server`
- **Type**: Counter
- **Description**: Number of successful CAS hits during worker-to-worker blob transfers.

**cas_miss**
- **Component**: `buildfarm-server`
- **Type**: Counter
- **Description**: Number of CAS misses during worker-to-worker blob transfers.

**cas_size**
- **Component**: `buildfarm-worker`
- **Type**: Gauge
- **Unit**: Bytes
- **Description**: Current total size of the worker's CAS cache in bytes.

**cas_entry_count**
- **Component**: `buildfarm-worker`
- **Type**: Gauge
- **Description**: Current total number of entries stored in the worker's CAS cache.

**cas_ttl_s**
- **Component**: `buildfarm-worker`
- **Type**: Histogram
- **Unit**: Seconds
- **Description**: Lifetime of CAS entries in L1 storage before expiration.

**expired_key**
- **Component**: `buildfarm-worker`
- **Type**: Counter
- **Description**: Total number of key/entry expirations in the CAS cache.

**read_io_errors**
- **Component**: `buildfarm-worker`
- **Type**: Counter
- **Description**: Number of I/O errors encountered while reading from the CAS.

**cas_copy_fallback**
- **Component**: `buildfarm-worker`
- **Type**: Counter
- **Description**: Number of times the CAS fell back to a file copy because hardlinking failed.

**io_bytes_read**
- **Component**: `buildfarm-server`, `buildfarm-worker`
- **Type**: Histogram / Counter
- **Unit**: Bytes
- **Description**: Bytes read during I/O operations.

**io_bytes_write**
- **Component**: `buildfarm-server`, `buildfarm-worker`
- **Type**: Histogram
- **Unit**: Bytes
- **Description**: Bytes written during ByteStream write operations.

### Worker Pipeline and Execution Metrics

**worker_paused**
- **Component**: `buildfarm-worker`
- **Type**: Counter
- **Description**: Number of times the worker has transitioned into a paused state.

**completed_operations**
- **Component**: `buildfarm-worker`
- **Type**: Counter
- **Description**: Total number of completed operations executed on the worker.

**execution_slot_usage**
- **Component**: `buildfarm-worker`
- **Type**: Gauge
- **Description**: Current number of execution slots in use on the worker.

**execution_slots_total**
- **Component**: `buildfarm-worker`
- **Type**: Gauge
- **Description**: Total number of execution slots configured on the worker (`executeStageWidth`).

**execution_time_ms**
- **Component**: `buildfarm-worker`
- **Type**: Histogram
- **Unit**: Milliseconds
- **Description**: Action execution duration on the worker.

**execution_stall_time_ms**
- **Component**: `buildfarm-worker`
- **Type**: Histogram
- **Unit**: Milliseconds
- **Description**: Action execution stall time on the worker.

**input_fetch_slot_usage**
- **Component**: `buildfarm-worker`
- **Type**: Gauge
- **Description**: Current number of input fetch slots in use on the worker.

**input_fetch_slots_total**
- **Component**: `buildfarm-worker`
- **Type**: Gauge
- **Description**: Total number of input fetch slots configured on the worker (`inputFetchStageWidth`).

**input_fetch_time_ms**
- **Component**: `buildfarm-worker`
- **Type**: Histogram
- **Unit**: Milliseconds
- **Description**: Time spent fetching inputs for action execution.

**input_fetch_stall_time_ms**
- **Component**: `buildfarm-worker`
- **Type**: Histogram
- **Unit**: Milliseconds
- **Description**: Input fetch stall time on the worker.

**report_result_slot_usage**
- **Component**: `buildfarm-worker`
- **Type**: Gauge
- **Description**: Current number of report result slots in use on the worker.

**report_result_slots_total**
- **Component**: `buildfarm-worker`
- **Type**: Gauge
- **Description**: Total number of report result slots configured on the worker (`reportResultStageWidth`).

**report_result_time_ms**
- **Component**: `buildfarm-worker`
- **Type**: Histogram
- **Unit**: Milliseconds
- **Description**: Time spent reporting action execution results.

**report_result_stall_time_ms**
- **Component**: `buildfarm-worker`
- **Type**: Histogram
- **Unit**: Milliseconds
- **Description**: Result reporting stall time on the worker.

**fetched_bytes_total**
- **Component**: `buildfarm-worker`
- **Type**: Counter
- **Unit**: Bytes
- **Description**: Total number of bytes fetched for executions on the worker.

**created_linked_directories_total**
- **Component**: `buildfarm-worker`
- **Type**: Counter
- **Description**: Total number of novel linked input directories created for executions.

**zstd_buffer_pool_used**
- **Component**: `buildfarm-worker`
- **Type**: Gauge
- **Description**: Current number of active Zstd decompression buffers in use from the worker's buffer pool.

### Local Resource Set Metrics

**local_resource_usage**
- **Component**: `buildfarm-worker`
- **Type**: Gauge
- **Labels**: `resource_name`
- **Description**: Number of claims currently in use for each configured local resource (e.g., GPU, memory).

**local_resource_total**
- **Component**: `buildfarm-worker`
- **Type**: Gauge
- **Labels**: `resource_name`
- **Description**: Total number of claims configured and available for each local resource.

**local_resource_requesters**
- **Component**: `buildfarm-worker`
- **Type**: Gauge
- **Labels**: `resource_name`
- **Description**: Number of actions currently requesting or claiming local resources.

### Worker Reindexing Metrics

**cas_indexer_removed_keys**
- **Component**: `buildfarm-server`
- **Type**: Gauge
- **Labels**: `node`
- **Description**: Number of CAS keys removed from Redis per node when reindexing departed workers.

**cas_indexer_removed_hosts**
- **Component**: `buildfarm-server`
- **Type**: Gauge
- **Labels**: `node`
- **Description**: Number of host entries removed from Redis per node when reindexing departed workers.

