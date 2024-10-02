---
layout: default
title: Instance Types
parent: Architecture
nav_order: 1
---

# Instance Types

These are the supported instance types selectable for concrete definition of an instance declared for servers in config. The types here are implementations of the [Instance](https://github.com/buildfarm/buildfarm/blob/main/src/main/java/build/buildfarm/instance/Instance.java) interface.

## Memory

This is a reference implementation of the Remote Execution API which provides an in-memory CAS, AC, and OperationQueue. It has no persistent retention of its state internally, but can be configured to use an external gRPC endpoint for CAS operations, allowing it to act as a proxy. Instances of this type cannot share Operation information across multiple servers/hosts. It presents a matching interface for workers via an OperationQueue service definition, which provides blocking queue `take` functionality, as well as a `put` for results, and it maintains watchdogs for all outstanding operations, with expiration resulting in reentrance to the queue, assuming that all input preconditions are still met at that time.

## Shard

The shard instance type is a frontend for a set of common backplane operations, allowing for wide distribution of retention and execution. The backplane serves as a registry of shard workers, the storage for the AC, the various queues and event monitors used in Operation processing, and the CAS index.

The only current backplane implementation uses redis, but the backplane interface is strongly decoupled from its usage, and any single or composite communication layer may be used to satisfy its requirements.

Sharded instances select arbitrary members of the Workers set for CAS writes, and reference the backplane CAS index for CAS reads, selecting any of a set of shards that advertise content to retrieve it. These shards can be any gRPC CAS compatible service endpoint.

Executions on shard are transformed into a worker queue through a processor built into the instance. A server which runs the instance (configurably) participates in this pool, reducing load on a directly connected service to the client. This allows an entire cluster of servers to participate evenly in populating the worker queue with heavyweight operation definitions, even if a client is only communicating with a single host.
