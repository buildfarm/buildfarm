---
layout: default
title: Configuration
nav_order: 4
has_children: true
---

Minimal required:

```yaml
backplane:
  redisUri: "redis://localhost:6379"
  queues:
    - name: "cpu"
      properties:
        - name: "min-cores"
          value: "*"
        - name: "max-cores"
          value: "*"
worker:
  publicName: "localhost:8981"
```

The configuration can be provided to the server and worker as a CLI argument or through the environment variable `CONFIG_PATH`
For an example configuration containing all of the configuration values, see `examples/config.yml`.

## All Configurations

### Common

| Configuration                | Accepted and _Default_ Values | Command Line Argument | Description                                                  |
|------------------------------|-------------------------------|-----------------------|--------------------------------------------------------------|
| digestFunction               | _SHA256_, SHA1                |                       | Digest function for this implementation                      |
| defaultActionTimeout         | Integer, _600_                |                       | Default timeout value for an action (seconds)                |
| maximumActionTimeout         | Integer, _3600_               |                       | Maximum allowed action timeout (seconds)                     |
| maxEntrySizeBytes            | Long, _2147483648_            |                       | Maximum size of a single blob accepted (bytes)               |
| prometheusPort               | Integer, _9090_               | --prometheus_port     | Listening port of the Prometheus metrics endpoint            |
| allowSymlinkTargetAbsolute   | boolean, _false_              |                       | Permit inputs to contain symlinks with absolute path targets |

Example:

```yaml
digestFunction: SHA1
defaultActionTimeout: 1800
maximumActionTimeout: 1800
prometheusPort: 9090
server:
  ...
worker:
  ...
```

### Server

| Configuration                    | Accepted and _Default_ Values | Environment Var | Description                                                                                                                 |
|----------------------------------|-------------------------------|-----------------|-----------------------------------------------------------------------------------------------------------------------------|
| instanceType                     | _SHARD_                       |                 | Type of implementation (SHARD is the only one supported)                                                                    |
| name                             | String, _shard_               |                 | Implementation name                                                                                                         |
| publicName                       | String, _DERIVED:port_        | INSTANCE_NAME   | Host:port of the GRPC server, required to be accessible by all servers                                                      |
| actionCacheReadOnly              | boolean, _false_              |                 | Allow/Deny writing to action cache                                                                                          |
| port                             | Integer, _8980_               |                 | Listening port of the GRPC server                                                                                           |
| casWriteTimeout                  | Integer, _3600_               |                 | CAS write timeout (seconds)                                                                                                 |
| bytestreamTimeout                | Integer, _3600_               |                 | Byte Stream write timeout (seconds)                                                                                         |
| sslCertificatePath               | String, _null_                |                 | Absolute path of the SSL certificate (if TLS used)                                                                          |
| sslPrivateKeyPath                | String, _null_                |                 | Absolute path of the SSL private key (if TLS used)                                                                          |
| runDispatchedMonitor             | boolean, _true_               |                 | Enable an agent to monitor the operation store to ensure that dispatched operations with expired worker leases are requeued |
| dispatchedMonitorIntervalSeconds | Integer, _1_                  |                 | Dispatched monitor's lease expiration check interval (seconds)                                                              |
| runOperationQueuer               | boolean, _true_               |                 | Acquire execute request entries cooperatively from an arrival queue on the backplane                                         |
| ensureOutputsPresent             | boolean, _false_              |                 | Decide if all outputs are also present in the CAS. If any outputs are missing a cache miss is returned                      |
| maxCpu                           | Integer, _0_                  |                 | Maximum number of CPU cores that any min/max-cores property may request (0 = unlimited)                                     |
| maxRequeueAttempts               | Integer, _5_                  |                 | Maximum number of requeue attempts for an operation                                                                         |
| useDenyList                      | boolean, _true_               |                 | Allow usage of a deny list when looking up actions and invocations (for cache only it is recommended to disable this check) |
| grpcTimeout                      | Integer, _3600_               |                 | GRPC request timeout (seconds)                                                                                              |
| executeKeepaliveAfterSeconds     | Integer, _60_                 |                 | Execute keep alive (seconds)                                                                                                |
| recordBesEvents                  | boolean, _false_              |                 | Allow recording of BES events                                                                                               |
| clusterId                        | String, _local_               |                 | Buildfarm cluster ID                                                                                                        |
| cloudRegion                      | String, _us-east_1_           |                 | Deployment region in the cloud                                                                                              |
| gracefulShutdownSeconds          | Integer, 0                    |                 | Time in seconds to allow for connections in flight to finish when shutdown signal is received                               |


Example:

```yaml
server:
  instanceType: SHARD
  name: shard
  actionCacheReadOnly: true
  recordBesEvents: true
```

### GRPC Metrics

| Configuration            | Accepted and _Default_ Values | Description                                            |
|--------------------------|-------------------------------|--------------------------------------------------------|
| enabled                  | boolean, _false_              | Publish basic GRPC metrics to a Prometheus endpoint    |
| provideLatencyHistograms | boolean, _false_              | Publish detailed, more expensive to calculate, metrics |
| labelsToReport           | List of Strings, _[]_         | Include custom metrics labels in Prometheus metrics    |

Example:

```yaml
server:
  grpcMetrics:
    enabled: false
    provideLatencyHistograms: false
    labelsToReport: []
```

### Server Caches

| Configuration                         | Accepted and _Default_ Values | Description                                                          |
|---------------------------------------|-------------------------------|----------------------------------------------------------------------|
| directoryCacheMaxEntries              | Long, _64 * 1024_             | The max number of entries that the directory cache will hold.        |
| commandCacheMaxEntries                | Long, _64 * 1024_             | The max number of entries that the command cache will hold.          |
| digestToActionCacheMaxEntries         | Long, _64 * 1024_             | The max number of entries that the digest-to-action cache will hold. |
| recentServedExecutionsCacheMaxEntries | Long, _64 * 1024_             | The max number of entries that the executions cache will hold.       |

Example:

```yaml
server:
  caches:
    directoryCacheMaxEntries: 10000
    commandCacheMaxEntries: 10000
    digestToActionCacheMaxEntries: 10000
    recentServedExecutionsCacheMaxEntries: 10000
```

### Admin

| Configuration         | Accepted and _Default_ Values | Description                                                                    |
|-----------------------|-------------------------------|--------------------------------------------------------------------------------|
| deploymentEnvironment | String, AWS, GCP              | Specify deloyment environment in the cloud                                     |
| clusterEndpoint       | String, grpc://localhost      | Buildfarm cluster endpoint for Admin use (this is a full buildfarm endpoint)   |

Example:

```yaml
server:
  admin:
    deploymentEnvironment: AWS
    clusterEndpoint: "grpc://localhost"
```

### Metrics

| Configuration       | Accepted and _Default_ Values | Description                                                                               |
|---------------------|-------------------------------|-------------------------------------------------------------------------------------------|
| publisher           | String, aws, gcp, _log_       | Specify publisher type for sending metadata                                               |
| logLevel            | String, INFO, _FINEST_        | Specify log level ("log" publisher only, all Java util logging levels are allowed here)   |
| topic               | String, _test_                | Specify SNS topic name for cloud publishing ("aws" publisher only)                        |
| topicMaxConnections | Integer, 1000                 | Specify maximum number of connections allowed for cloud publishing ("aws" publisher only) |
| secretName          | String, _test_                | Specify secret name to pull SNS permissions from ("aws" publisher only)                   |

Example:

```yaml
server:
  metrics:
    publisher: log
    logLevel: INFO
```

```yaml
server:
  metrics:
    publisher: aws
    topic: buildfarm-metadata-test
    topicMaxConnections: 1000
    secretName: buildfarm-secret
```

### Redis Backplane

| Configuration                | Accepted and _Default_ Values            | Environment Var | Command Line Argument | Description                                                                                                                                                                                  |
|------------------------------|------------------------------------------|-----------------|-----------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| type                         | _SHARD_                                  |                 |                       | Type of backplane. Currently, the only implementation is SHARD utilizing Redis                                                                                                               |
| redisUri                     | String, redis://localhost:6379           | REDIS_URI       | --redis_uri           | Redis cluster endpoint. This must be a single URI. This can embed a username/password per RFC-3986 Section 3.2.1 and this will take precedence over `redisPassword` and `redisPasswordFile`. |
| redisPassword                | String, _null_                           |                 |                       | Redis password, if applicable                                                                                                                                                                |
| redisPasswordFile            | String, _null_                           |                 |                       | File to read for a Redis password. If specified, this takes precedence over `redisPassword`                                                                                                  |
| redisNodes                   | List of Strings, _null_                  |                 |                       | List of individual Redis nodes, if applicable                                                                                                                                                |
| jedisPoolMaxTotal            | Integer, _4000_                          |                 |                       | The size of the Redis connection pool                                                                                                                                                        |
| workersHashName              | String, _Workers_                        |                 |                       | Redis key used to store a hash of registered workers                                                                                                                                         |
| workerChannel                | String, _WorkerChannel_                  |                 |                       | Redis pubsub channel key where changes of the cluster membership are announced                                                                                                               |
| actionCachePrefix            | String, _ActionCache_                    |                 |                       | Redis key prefix for all ActionCache entries                                                                                                                                                 |
| actionCacheExpire            | Integer, _2419200_                       |                 |                       | The TTL maintained for ActionCache entries, not refreshed on getActionResult hit                                                                                                             |
| actionBlacklistPrefix        | String, _ActionBlacklist_                |                 |                       | Redis key prefix for all blacklisted actions, which are rejected                                                                                                                             |
| actionBlacklistExpire        | Integer, _3600_                          |                 |                       | The TTL maintained for action blacklist entries                                                                                                                                              |
| invocationBlacklistPrefix    | String, _InvocationBlacklist_            |                 |                       | Redis key prefix for blacklisted invocations, suffixed with a a tool invocation ID                                                                                                           |
| operationPrefix              | String, _Operation_                      |                 |                       | Redis key prefix for all operations, suffixed with the operation's name                                                                                                                      |
| operationExpire              | Integer, _604800_                        |                 |                       | The TTL maintained for all operations, updated on each modification                                                                                                                          |
| preQueuedOperationsListName  | String, _{Arrival}:PreQueuedOperations_  |                 |                       | Redis key used to store a list of ExecuteEntry awaiting transformation into QueryEntry                                                                                                       |
| processingListName           | String, _{Arrival}:ProcessingOperations_ |                 |                       | Redis key of a list used to ensure reliable processing of arrival queue entries with operation watch monitoring                                                                              |
| processingPrefix             | String, _Processing_                     |                 |                       | Redis key prefix for operations which are being dequeued from the arrival queue                                                                                                              |
| processingTimeoutMillis      | Integer, _20000_                         |                 |                       | Delay (in ms) used to populate processing operation entries                                                                                                                                  |
| queuedOperationsListName     | String, _{Execution}:QueuedOperations_   |                 |                       | Redis key used to store a list of QueueEntry awaiting execution by workers                                                                                                                   |
| dispatchingPrefix            | String, _Dispatching_                    |                 |                       | Redis key prefix for operations which are being dequeued from the ready to run queue                                                                                                         |
| dispatchingTimeoutMillis     | Integer, _10000_                         |                 |                       | Delay (in ms) used to populate dispatching operation entries                                                                                                                                 |
| dispatchedOperationsHashName | String, _DispatchedOperations_           |                 |                       | Redis key of a hash of operation names to the worker lease for its execution, which are monitored by the dispatched monitor                                                                  |
| operationChannelPrefix       | String, _OperationChannel_               |                 |                       | Redis pubsub channel prefix suffixed by an operation name                                                                                                                                    |
| casPrefix                    | String, _ContentAddressableStorage_      |                 |                       | Redis key prefix suffixed with a blob digest that maps to a set of workers with that blob's availability                                                                                     |
| casExpire                    | Integer, _604800_                        |                 |                       | The TTL maintained for CAS entries, which is not refreshed on any read access of the blob                                                                                                    |
| subscribeToBackplane         | boolean, _true_                          |                 |                       | Enable an agent of the backplane client which subscribes to worker channel and operation channel events. If disabled, responsiveness of watchers and CAS are reduced                         |
| runFailsafeOperation         | boolean, _true_                          |                 |                       | Enable an agent in the backplane client which monitors watched operations and ensures they are in a known maintained, or expirable state                                                     |
| maxQueueDepth                | Integer, _100000_                        |                 |                       | Maximum length that the ready to run queue is allowed to reach to control an arrival flow for execution                                                                                      |
| maxPreQueueDepth             | Integer, _1000000_                       |                 |                       | Maximum lengh that the arrival queue is allowed to reach to control load on the Redis cluster                                                                                                |
| priorityQueue                | boolean, _false_                         |                 |                       | Priority queue type allows prioritizing operations based on Bazel's --remote_execution_priority=<an integer> flag                                                                            |
| timeout                      | Integer, _10000_                         |                 |                       | Default timeout                                                                                                                                                                              |
| maxInvocationIdTimeout       | Integer, _604800_                        |                 |                       | Maximum TTL (Time-to-Live in second) of invocationId keys in RedisBackplane                                                                                                                  |
| maxAttempts                  | Integer, _20_                            |                 |                       | Maximum number of execution attempts                                                                                                                                                         |
| cacheCas                     | boolean, _false_                         |                 |                       |                                                                                                                                                                                              |

Example:

```yaml
backplane:
  type: SHARD
  redisUri: "redis://localhost:6379"
  priorityQueue: true
```

### Execution Queues

| Configuration  | Accepted and _Default_ Values | Description                                                                                               |
|----------------|-------------------------------|-----------------------------------------------------------------------------------------------------------|
| name           | String                        | Name of the execution queue (ex: cpu, gpu)                                                                |
| allowUnmatched | boolean, _true_               |                                                                                                           |
| properties     | List of name/value pairs      | Any specification of min/max-cores will be allowed to support CPU controls and worker resource delegation |

Example:

```yaml
backplane:
  type: SHARD
  redisUri: "redis://localhost:6379"
  queues:
    - name: "cpu"
      allowUnmatched: true
      properties:
        - name: "min-cores"
          value: "*"
        - name: "max-cores"
          value: "*"
```

### Worker

| Configuration                    | Accepted and _Default_ Values | Environment Var       | Description                                                                                                                                                                                                                                                                                                              |
|----------------------------------|-------------------------------|-----------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| port                             | Integer, _8981_               |                       | Listening port of the worker                                                                                                                                                                                                                                                                                             |
| publicName                       | String, _DERIVED:port_        | INSTANCE_NAME         | Host:port of the GRPC server, required to be accessible by all servers                                                                                                                                                                                                                                                   |
| root                             | String, _/tmp/worker_         |                       | Path for all operation content storage                                                                                                                                                                                                                                                                                   |
| inlineContentLimit               | Integer, _1048567_            |                       | Total size in bytes of inline content for action results, output files, stdout, stderr content                                                                                                                                                                                                                           |
| operationPollPeriod              | Integer, _1_                  |                       | Period between poll operations at any stage                                                                                                                                                                                                                                                                              |
| executeStageWidth                | Integer, _0_                  | EXECUTION_STAGE_WIDTH | Number of CPU cores available for execution (0 = system available cores)                                                                                                                                                                                                                                                 |
| executeStageWidthOffset          | Integer, _0_                  |                       | Offset number of CPU cores available for execution (to allow for use by other processes)                                                                                                                                                                                                                                 |
| inputFetchStageWidth             | Integer, _0_                  |                       | Number of concurrently available slots to fetch inputs (0 = system calculated based on CPU cores)                                                                                                                                                                                                                        |
| inputFetchDeadline               | Integer, _60_                 |                       | Limit on time (seconds) for input fetch stage to fetch inputs                                                                                                                                                                                                                                                            |
| linkInputDirectories             | boolean, _true_               |                       | Use an input directory creation strategy which creates a single directory tree at the highest level containing no output paths of any kind, and symlinks that directory into an action's execroot, saving large amounts of time spent manufacturing the same read-only input hierirchy over multiple actions' executions |
| execOwner                        | String, _null_                |                       | Create exec trees containing directories that are owned by this user                                                                                                                                                                                                                                                     |
| hexBucketLevels                  | Integer, _0_                  |                       | Number of levels to create for directory storage by leading byte of the hash (problematic, not recommended)                                                                                                                                                                                                              |
| defaultMaxCores                  | Integer, _0_                  |                       | Constrain all executions to this logical core count unless otherwise specified via min/max-cores (0 = no limit)                                                                                                                                                                                                          |
| limitGlobalExecution             | boolean, _false_              |                       | Constrain all executions to a pool of logical cores specified in executeStageWidth                                                                                                                                                                                                                                       |
| onlyMulticoreTests               | boolean, _false_              |                       | Only permit tests to exceed the default coresvalue for their min/max-cores range specification (only works with non-zero defaultMaxCores)                                                                                                                                                                                |
| allowBringYourOwnContainer       | boolean, _false_              |                       | Enable execution in a custom Docker container                                                                                                                                                                                                                                                                            |
| errorOperationRemainingResources | boolean, _false_              |                       |                                                                                                                                                                                                                                                                                                                          |
| errorOperationOutputSizeExceeded | boolean, _false_              |                       | Operations which produce single output files which exceed maxEntrySizeBytes will fail with a violation type which implies a user error. When disabled, the violation will indicate a transient error, with the action blacklisted.                                                                                       |
| realInputDirectories             | List of Strings, _external_   |                       | A list of paths that will not be subject to the effects of linkInputDirectories setting, may also be used to provide writable directories as input roots for actions which expect to be able to write to an input location and will fail if they cannot                                                                  |
| gracefulShutdownSeconds          | Integer, 0                    |                       | Time in seconds to allow for operations in flight to finish when shutdown signal is received                                                                                                                                                                                                                             |
| createSymlinkOutputs             | boolean, _false_              |                       | Creates SymlinkNodes for symbolic links discovered in output paths for actions. No verification of the symlink target path occurs. Buildstream, for example, requires this.                                                                                                                                              |

```yaml
worker:
  port: 8981
  publicName: "localhost:8981"
  realInputDirectories:
    - "external"
```

### Capabilities

| Configuration | Accepted and _Default_ Values | Description                                     |
|---------------|-------------------------------|-------------------------------------------------|
| cas           | boolean, _true_               | Enables worker to be a shard of the CAS         |
| execution     | boolean, _true_               | Enables worker to participate in execution pool |

Example:

```yaml
worker:
  capabilities:
    cas: true
    execution: true
```

### Sandbox Settings

| Configuration | Accepted and _Default_ Values | Description                                          |
|---------------|-------------------------------|------------------------------------------------------|
| alwaysUseSandbox      | boolean, _false_      | Enforce that the sandbox be used on every acion.     |
| alwaysUseCgroups      | boolean, _true_       | Enforce that actions run under cgroups.              |
| alwaysUseTmpFs        | boolean, _false_      | Enforce that the sandbox uses tmpfs on every acion.  |
| selectForBlockNetwork | boolean, _false_      | `block-network` enables sandbox action execution.    |
| selectForTmpFs        | boolean, _false_      | `tmpfs` enables sandbox action execution.            |

Example:

```yaml
worker:
  sandboxSettings:
    alwaysUseSandbox: true
    alwaysUseCgroups: true
    alwaysUseTmpFs: true
    selectForBlockNetwork: false
    selectForTmpFs: false
```

Note: In order for these settings to take effect, you must also configure `limitGlobalExecution: true`.

### Dequeue Match

| Configuration    | Accepted and _Default_ Values | Description |
|------------------|-------------------------------|-------------|
| allowUnmatched   | boolean, _false_              |             |

Example:

```yaml
worker:
  dequeueMatchSettings:
    allowUnmatched: false
```

### Worker CAS

Unless specified, options are only relevant for FILESYSTEM type

| Configuration                | Accepted and _Default_ Values | Description                                                                                                   |
|------------------------------|-------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------|
| type                         | _FILESYSTEM_, GRPC            | Type of CAS used                                                                                                                                   |
| path                         | String, _cache_               | Local cache location relative to the 'root', or absolute                                                                                           |
| maxSizeBytes                 | Integer, _0_                  | Limit for contents of files retained from CAS in the cache, value of 0 means to auto-configure to 90% of _root_/_path_ underlying filesystem space |
| fileDirectoriesIndexInMemory | boolean, _false_              | Determines if the file directories bidirectional mapping should be stored in memory or in sqlite                                                  |
| skipLoad                     | boolean, _false_              | Determines if transient data on the worker should be loaded into CAS on worker startup (affects startup time)                                |
| target                       | String, _null_                | For GRPC CAS type, target for external CAS endpoint                                                                                                |

Example:

This definition will create a filesystem-based CAS file cache at the path "<root>/cache" on the worker that will reject entries over 2GiB in size, and will expire LRU blobs when the aggregate size of all blobs exceeds 2GiB in order to insert additional entries.

```yaml
worker:
  storages:
    - type: FILESYSTEM
      path: "cache"
      maxSizeBytes: 2147483648 # 2 * 1024 * 1024 * 1024
      maxEntrySizeBytes: 2147483648 # 2 * 1024 * 1024 * 1024
```

This definition elides FILESYSTEM configuration with '...', will read-through an external GRPC CAS supporting the REAPI CAS Services into its storage, and will attempt to write expiring entries into the GRPC CAS (i.e. pushing new entries into the head of a worker LRU list will drop the entries from the tail into the GRPC CAS).

```
worker:
  storages:
    - type: FILESYSTEM
      ...
    - type: GRPC
      target: "cas.external.com:1234"
```

### Execution Policies

| Configuration    | Accepted and _Default_ Values                              | Description                                                         |
|------------------|------------------------------------------------------------|---------------------------------------------------------------------|
| name             | String                                                     | Execution policy name                                               |
| executionWrapper | Execution wrapper, containing a path and list of arguments | Execution wrapper, its path and a list of arguments for the wrapper |

Example:

```yaml
worker:
  executionPolicies:
    - name: test
      executionWrapper:
        path: /
        arguments:
          - arg1
          - arg2
```
