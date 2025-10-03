---
layout: default
title: Debugging
parent: Execution
nav_order: 1
---

# Debugging Executions

This tutorial is intended to guide you in debugging executions that are run on buildfarm.

### Problem
Build actions (and more specifically unit tests) often behave differently across environments.  This makes it difficult to understand and debug remote action behavior.  The execution environment of buildfarm may seem like an inaccessible black-box to the client.  The problem is further complicated by the fact that buildfarm's executor may not be using the same tools locally as the build system (such as bazel's sandbox or process-wrapper).  In buildfarm, these tools are chosen based on the configuration of execution wrappers, and custom execution wrappers exist as well.  Buildfarm may also virtualize hardware and apply restrictions on resources such as cpu, memory, and network.  Additionally, buildfarm actions do not always run on the same machine. Actions in buildfarm are directed to particular eligible workers based on their platform properties.  Despite the platform properties of the worker chosen, the action may run inside a docker container which also affects the execution environment.

### Capturing debug information (before execution)
There are execution properties available to help you capture debug information dynamically when performing executions.
To capture initial information about an execution, you can use the following:
```
bazel test --remote_default_exec_properties='debug-before-execution=true'  \
--remote_executor=grpc://127.0.0.1:8980 \
--noremote_accept_cached  \
--nocache_test_results
//<TARGET>
```

Assuming the action is properly queued and reaches the executor, the executor will deliberately fail the action and send you debug information via json through the action's stderr.
You can view this debug information by reading the failure log:
```
cat source/bazel-out/k8-fastbuild/testlogs/<TARGET>/test.log
```

You can streamline viewing this information via `--test_output=streamed`
```
bazel test \
--test_output=streamed \
--remote_default_exec_properties='debug-before-execution=true' \
--noremote_accept_cached \
--nocache_test_results \
//<TARGET>
```

### Capturing debug information (after execution)

A similar property exists that will tell the executor to run the action, and after the action finishes, return debug information.
```
bazel test \
--test_output=streamed \
--remote_default_exec_properties='debug-after-execution=true' \
--noremote_accept_cached \
--nocache_test_results \
//<TARGET>
```

If your action is hanging, you might not be able to get this debug information and should stick with 'debug-before-execution=true'.
If your action is finishing, this would be a ideal to use, as it should provide all the same information that 'debug-before-execution=true' does, with additional info about the execution.

### Configuration
If you see an error like this:
```
properties are not valid for queue eligibility: [name: "debug-before-execution" value: "true"]
```
you will need to configure the queue's `allow_unmatched` to true so that the server can still put the action on the queue.
Additionally you may want to configure the worker's `DequeueMatchSettings` to also have `allow_unmatched` to true.  That will ensure the action does not fail reaching one of the worker due to the additional exec_property.

### Debugging Tests
By default, `debug-before-execution` and `debug-after-execution` only apply to test actions.  The reason being, is that if you wanted to debug a test, but passed a global property like `--remote_default_exec_properties='debug-before-execution=true'`, it would invalidate all the actions, and the test would need rebuilt, but the rebuild of the test would fail, because you would actually be debugging the first build action, and you would never see the debug results of the test action.  Debugging tests is more typical, but you can also debug build actions by using `--remote_default_exec_properties='debug-tests-only=false'`.

It is more convenient to debug things by passing the global exec properties, but you could also tag targets specifically in the BUILD files with these debug options.

### Finding Operations
All buildfarm operations can be found using the following query:
```
bazel run //src/main/java/build/buildfarm:bf-find-operations localhost:8980 shard SHA256
```

When you run a build invocation with bazel, bazel will you give you an invocation id.  You can use that to query your specific operations:
```
bazel run //src/main/java/build/buildfarm:bf-find-operations localhost:8980 shard SHA256 "[?(@.operation.metadata.requestMetadata.toolInvocationId == '1877f43a-9b33-4eca-9d6b-aef71b47bf47')]"
```

You can find the operation of a specific test name like this:
```
bazel run //src/main/java/build/buildfarm:bf-find-operations localhost:8980 shard SHA256 "$.command.environmentVariables[?(@.value == '//code/tools/example_tests/bash_hello_world2:main')]"
```