---
layout: default
title: Limiting
parent: Execution
nav_order: 3
---

The ExecuteActionStage on Shard Workers can limit the resources available during Command execution per the Platform.

This feature currently requires OS-specific `cgroups` support, available in Linux.

The boolean configuration field `limit_execution` controls the creation of a worker-wide cgroup. This cgroup currently limits cpu resources to the the ExecuteStageWidth (one core per slot), and other worker-wide cgroup controls may be available in the future. Other limiting fields also require this option to be enabled. This does not mandate that all executions take place in this created cgroup.

If true, the boolean configuration field `limit_global_execution` specifies that all executions take place in the above cgroup by default. This currently shrinks the effective cpu area available to be shared by all executions to the width of the execute stage.

With `limit_execution` enabled, a Command may request that it be executed with `min-cores` available to it (lower bound) and up to `max-cores`. These are field `name`s in the Platform which are expected to be integers in the string `value`. After selective consumption of the superscalar execution slots for `min-cores`, the executing processes will be controlled by cgroups **under** the above cgroup to give it as many shares of the total (preferred scheduling) for `min-cores`, and have its scheduling quota limited to a fair number consistent with its `max-cores`.

The effective Platform of an execution can also be affected by the Worker's `default_platform`, where it can inherit either of `min/max-cores` if unspecified.

The Worker configuration field `only_multicore_tests` can further affect the values of `min/max-cores` by clamping them to 1 if enabled, based on the detection of 'bazel-like' test environments for executions: The detection of an environment variable XML_OUTPUT_FILE under the command is the current qualifier