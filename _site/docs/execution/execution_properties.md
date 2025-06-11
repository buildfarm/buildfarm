---
layout: default
title: Properties
parent: Execution
nav_order: 4
---

This page contains all of the [execution properties](https://docs.bazel.build/versions/master/be/common-definitions.html#common.exec_properties) supported by Buildfarm.
Users can also customize buildfarm to understand additional properties that are not listed here (This is often done when configuring the [Operation Queue](https://github.com/buildfarm/buildfarm/wiki/Operation-Queue)).


## Core Selection:

### `min-cores`
**description:** the minimum number of cores needed by an action.  Should be set to >= 1
Workers and queues can be configured to behave differently based on this property.

### `max-cores`
**description:** the maximum number of cores needed by an action. Buildfarm will enforce a max.
Workers and queues can be configured to behave differently based on this property.

### `cores`
**description:** the minimum & maximum number of cores needed by an action.  This sets both `min-cores` and `max-cores` accordingly.

**use case:** very often you want unit tests (or all actions in general) to be constrained to a core limit via cgroups.
This is relevant for performance and stability of the worker as multiple tests share the same hardware as the worker.

## Memory Selection:

### `min-mem`
**description:** the minimum amount of bytes the action may use.

### `max-mem`
**description:** the maximum amount of bytes the action may use.

**use case:** very often you want unit tests (or all actions in general) to be constrained to a memory limit via cgroups.
This is relevant for performance and stability of the worker as multiple tests share the same hardware as the worker.
Tests that exceed their memory requirements will be killed.

## Execution Settings:

### `linux-sandbox`
**description:** Use bazel's linux sandbox as an execution wrapper.

### `fake-hostname`
**description:** Uses `localhost` as the hostname during execution.  Assumes the usage of the linux sandbox.

### `block-network`
**description:** Creates a new network namespace.  Assumes the usage of the linux sandbox.

### `tmpfs`
**description:** Mounts an empty tmpfs under `/tmp` for the action.  Assumes the usage of the linux sandbox.

## Queue / Pool Selection:

### `choose-queue`
**description:** place the action directly on the chosen queue (queue name must be known based on buildfarm configuration).

**use case:** Other remote execution solutions have slightly different paradigms on deciding where actions go. They leverage execution properties for selecting a "pool" of machines to send the action. We sort of have a pool of workers waiting on particular queues. For parity with this concept, we support this execution property which will take precedence in deciding queue eligibility.

## Extending Execution:

### `env-var` / `env-vars`
**description:** ensure the action is executed with additional environment variables.  These variables are applied last in the order given.

`env-var` expects a single key/value like `--remote_default_exec_properties=env-var:FOO=VALUE`
`env-vars` expects a key/json like `--remote_default_exec_properties=env-vars='{"FOO": "VALUE","FOO2": "VALUE2"}'`

**use case:**
Users may need to set additional environment variables through `exec_properties`.
Changing code or using `--action_env` may be less feasible than specifying them through these exec_properties.
Additionally, the values of their environment variables may need to be influenced by buildfarm decisions.

**example:** pytorch tests can still see the underlying hardware through `/proc/cpuinfo`.
Despite being given 1 core, they see all of the cpus and decide to spawn that many threads. This essentially starves them and gives poor test performance (we may spoof cpuinfo in the future).  Another solution is to use env vars `OMP_NUM_THREADS` and `MKL_NUM_THREADS`.  This could be done in code, but we can't trust that developers will do it consistently or keep it in sync with `min-cores` / `max-cores`.  Allowing these environment variables to be passed the same way as the core settings would be ideal.

**Standard Example:**
This test will succeed when env var TESTVAR is foobar, and fail otherwise.

```shell
#!/usr/bin/env bash
[ "$TESTVAR" = "foobar" ]
```

```shell
$ ./bazel test  \
--remote_executor=grpc://127.0.0.1:8980 --noremote_accept_cached  --nocache_test_results \
//env_test:main
FAIL
```

```shell
$ ./bazel test --remote_default_exec_properties='env-vars={"TESTVAR": "foobar"}' \
 --remote_executor=grpc://127.0.0.1:8980 --noremote_accept_cached  --nocache_test_results \
//env_test:main
PASS
```

**Template Example:**
If you give a range of cores, buildfarm has the authority to decide how many your operation actually claims.  You can let buildfarm resolve this value for you (via [mustache](https://mustache.github.io/)).
```bash
#!/usr/bin/env bash
[ "$MKL_NUM_THREADS" = "1" ]
```

```shell
$ ./bazel test  \
--remote_executor=grpc://127.0.0.1:8980 --noremote_accept_cached  --nocache_test_results \
//env_test:main
FAIL
```

```shell
$ ./bazel test  \
--remote_default_exec_properties='env-vars="MKL_NUM_THREADS": "{{limits.cpu.claimed}}"' \
--remote_executor=grpc://127.0.0.1:8980 --noremote_accept_cached  --nocache_test_results \
//env_test:main
PASS
```

**Available Templates:**
`{{limits.cpu.min}}`: what buildfarm has decided is a valid min core count for the action.
`{{limits.cpu.max}}`: what buildfarm has decided is a valid max core count for the action.
`{{limits.cpu.claimed}}`: buildfarm's decision on how many cores your action should claim.

## Debugging Execution:

### `debug-before-execution`
**description:** Fails the execution with important debug information on how the execution will be performed.
**use case:** Sometimes you want to know the exact execution context and cli that the action is going to be run with.  This can help any situation where local action behavior seems different than remote action behavior.

### `debug-after-execution`
**description:** Runs the execution, but fails it afterward with important debug information on how the execution was performed.


## Additional Information
Custom properties can also be added to buildfarm's configuration in order to facilitate queue matching (see [Queues](https://buildfarm.github.io/buildfarm/docs/architecture/queues/)).

Please note that not all execution properties may be relevant to you or the best option depending on your build client.
For example, some execution properties were created to facilitate behavior before bazel had a better solution in place.

Buildfarm's configuration for accepting execution properties can be strict or flexible.  Buildfarm has been used alongside other remote execution tools and allowing increased flexibility on these properties is necessary so the solutions can coexist for the same targets.
