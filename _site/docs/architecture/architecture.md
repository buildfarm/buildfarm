---
layout: default
title: Architecture
has_children: true
---

# Instances

## Definition

An instance is a namespace which represents a pool of resources available to a remote execution client. All requests to the Remote Execution Services are identified with an instance name, and a client is expected to communicate with the same instance between requests in order to accomplish aggregate activities. For instance, a typical client usage of FindMissingBlobs, then one or more Write/BatchUploadBlobs, Execute, and zero or more Read/BatchReadBlobs would all have the same instance name associated with the requests.

## Implementation

Buildfarm uses modular instances, where an instance is associated with one concrete type that governs its behavior and functionality. The Remote Execution API uses instance names to identify every request made, which allows Buildfarm instances to represent partitions of resources. One endpoint may support many different instances, each with their own name, and each instance will have its own type.

# Typical Deployment

![Buildfarm Architecture Diagram](https://github.com/bazelbuild/bazel-buildfarm/wiki/images/Architecture-Aws.png)

## Workers

Workers are deployed as an autoscaling group in the cloud environment. This group should scale based on the load. This will require monitoring and alerting to be setup, which will trigger the scaling events.

## Schedulers

Schedulers are deployed as an autoscaling group in the cloud environment. This group should scale based on the load. This will require monitoring and alerting to be setup, which will trigger the scaling events.

## Clustered Redis

Clustered Redis should be sized based on the expected load. Typically, no replication is necessary for Buildfarm use as the loss of data stored is not catastrophic.

## Schedulers Network Load Balancer

A network load balancer is set up to target the Schedulers autoscaling group. This will be the primary Buildfarm endpoint.