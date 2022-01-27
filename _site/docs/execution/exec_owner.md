---
layout: default
title: Owner
parent: Execution
nav_order: 2
---

`exec_owner` is a global worker configuration option for Shard workers which indicates a username on the worker that all output directories for executions will own. The InputFetchStage uses this value to specify the filesystem owner user id for Action->Command OutputFiles and OutputDirectories (soon to be OutputPaths in REAPI 2.1) paths created during exec root preparation.

Specification of this option requires that the filesystem ownership change is available to the worker process principal. For example, a worker which executes as `root` under Unix would be capable of changing directory ownership.

This option only affects file ownership, not effective user id of the action process. This option can be coupled with a default ExecutionPolicy wrapper that performs an execution persona change (i.e. `setuid(2)` under Unix), to execute a command with privileges that limit it in writing.