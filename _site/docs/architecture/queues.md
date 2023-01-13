---
layout: default
title: Queues
parent: Architecture
nav_order: 4
---

# Operation Queue
This section discusses the purpose and design of the Operation queue.  It also discusses how it can be customized depending on the type of operations you wish to support, and how you wish to distribute them among workers.

## Quick Summary
Some time after an Action execute request occurs, the longrunning operation it corresponds to will enter the QUEUED state, and will receive an update to that effect on the operation response stream. An operation in the QUEUED state is present in an Operation Queue, which holds the operations in sequence until a worker is available to execute it.

Schedulers put operations on the queue.  Workers take them off.
![Operation Queue]({{site.url}}{{site.baseurl}}/assets/images/Operation-Queue1.png)

## Working with different platform requirements
Some operations' Actions may have specific platform requirements in order to execute.
Likewise, specific workers may only want to take on work that they deem eligible.
To solve this, the operation queue can be customized to divide work into separate provisioned queues so that specific workers can choose which queue to read from.

Provision queues are intended to represent particular operations that should only be processed by particular workers. An example use case for this would be to have two dedicated provision queues for CPU and GPU operations. CPU/GPU requirements would be determined through the [remote api's command platform properties](https://github.com/bazelbuild/remote-apis/blob/main/build/bazel/remote/execution/v2/remote_execution.proto#L595). We designate provision queues to have a set of "required provisions" (which match the platform properties). This allows the scheduler to distribute operations by their properties and allows workers to dequeue from particular queues.

If your configuration file does not specify any provisioned queues, buildfarm will automatically provide a default queue with full eligibility on all operations.
This will ensure the expected behavior for the paradigm in which all work is put on the same queue.

###  Matching Algorithm
The matching algorithm is performed by the operation queue when the caller is requesting to push or pop elements.
The matching algorithm is designed to find the appropriate queue to perform these actions on.
On the scheduler side, the action's platform properties are used for matching.
On the worker side, the `dequeue_match_settings` are used.
![Operation Queue Matching]({{site.url}}{{site.baseurl}}/assets/images/Operation-Queue-Matching1.png)

This is how the matching algorithm works:
Each provision queue is checked in the order that it is configured.
The first provision queue that is deemed eligible is chosen and used.
When deciding if an action is eligible for the provision queue, each platform property is checked individually.
By default, there must be a perfect match on each key/value.
Wildcards ("*") can be used to avoid the need of a perfect match.
Additionally, if the action contains any platform properties is not mentioned by the provision queue, it will be deemed ineligible.
setting `allow_unmatched: true` can be used to allow a superset of action properties as long as a subset matches the provision queue.
If no provision queues can be matched, the operation queue will provide an analysis on why none of the queues were eligible.

When taking elements off of the operation queue, the matching algorithm behaves a similar way.
The worker's `DequeueMatchSettings` also have an `allow_unmatched` property.
Workers also have the ability to reject an operation after matching with a provision queue and dequeuing a value.
To avoid any of these rejections by the worker, you can use `accept_everything: true`.

When configuring your worker, consider the following decisions:
First, if the accept_everything setting is true, the job is accepted.
Otherwise, if any execution property for the queue has a wildcard key, the job is accepted.
Otherwise, if the allow_unmatched setting is true, each key present in the queue's properties must be a wildcard or exist in the execution request's properties with an equal value.
Otherwise, the execution request's properties must have exactly the same set of keys as the queue's execution properties, and the request's value for each property must equal the queue's if the queue's value for this property is not a wildcard.

### Server Example

In this example the scheduler declares a GPU queue and CPU queue. All queues must be declared for the server deployment:
```
backplane:
  queues:
    - name: "cpu"
      allowUnmatched: true
      properties:
        - name: "min-cores"
          value: "*"
        - name: "max-cores"
          value: "*"
    - name: "gpu"
      allowUnmatched: true
      properties:
        - name: "gpu"
          value: "1"
```

### Worker Example

Queues are defined similarly on Workers. Only the specific worker type queue must be declared for that specific worker deployment.

For example, for a CPU worker pool use:

```
backplane:
  queues:
    - name: "cpu"
      allowUnmatched: true
      properties:
        - name: "min-cores"
          value: "*"
        - name: "max-cores"
          value: "*"
```

For example, for a GPU worker pool use:

```
backplane:
  queues:
    - name: "gpu"
      allowUnmatched: true
      properties:
        - name: "gpu"
          value: "1"
```

Note: make sure that all workers can communicate with each other before trying these examples

### Bazel Perspective

Bazel targets can pass these platform properties to buildfarm via [exec_properties](https://docs.bazel.build/versions/master/be/common-definitions.html#common.exec_properties).
Here is for example how to run a remote build for the GPU queue example above:

```shell
bazel build --remote_executor=grpc://server:port --remote_default_exec_properties=gpu=1 //...
```
