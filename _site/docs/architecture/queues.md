---
layout: default
title: Queues
parent: Architecture
nav_order: 4
---

# Shard Platform Operation Queue
This section discusses the purpose of the Shard Platform Operation Queue, and how it can be customized depending on the type of operations you wish to support and efficiently distribute among workers.

Some time after an Action execute request occurs, the longrunning operation it corresponds to will enter the QUEUED state, and will receive an update to that effect on the operation response stream. An operation in the QUEUED state is present in an Operation Queue, which holds the operations in sequence until a worker is available to execute it.

## Working with different platform requirements 
Some operations' Actions may have specific platform requirements in order to execute.
Likewise, specific workers may only want to take on work that they deem eligible.
To solve this, the operation queue can be customized to divide work into separate provisioned queues so that specific workers can choose which queue to read from.  

Provision queues are intended to represent particular operations that should only be processed by particular workers. An example use case for this would be to have two dedicated provision queues for CPU and GPU operations. CPU/GPU requirements would be determined through the [remote api's command platform properties](https://github.com/bazelbuild/remote-apis/blob/86c040d03101654a949539151d32e22dfea30d62/build/bazel/remote/execution/v2/remote_execution.proto#L595). We designate provision queues to have a set of "required provisions" (which match the platform properties). This allows the scheduler to distribute operations by their properties and allows workers to dequeue from particular queues.

### Server Example
In this example the scheduler is configured to separate CPU/GPU work:
```
redis_shard_backplane_config: {
  provisioned_queues: {
    queues: [
      {
        name: "gpu_queue"
        platform: {
          properties: [{name: "gpu" value: "1"}]
        }
      },
      {
          name: "cpu_queue"
      }
    ]
  }
}
```

### Worker Example
Similarly a worker is configured to choose from CPU/GPU work:
```
redis_shard_backplane_config: {
  provisioned_queues: {
    queues: [
      {
        name: "gpu_queue"
        platform: {
          properties: [{name: "gpu" value: "1"}]
        }
      },
      {
          name: "cpu_queue"
      }
    ]
  }
}
```

Note that in both of these examples we specify the CPU queue last.  
An operation queue consists of multiple provisioned queues in which the order dictates the eligibility and placement of operations.  Therefore, it is recommended to have a final provision queue with no actual platform requirements.  This will ensure that all operations are eligible for the final queue.

### Tagging a Worker for GPU work
```
omit_from_cas: true

platform: {
    properties: [{name: "gpu" value: "1"}]
}
```

### Default Configuration
If your configuration file does not specify any provisioned queues, buildfarm will automatically provide a default queue will full eligibility on all operations. This will ensure the expected behavior for the paradigm in which all work is put on the same queue.

### Bazel Perspective
Bazel targets can pass these platform properties to buildfarm via [exec_properties](https://docs.bazel.build/versions/master/be/common-definitions.html#common.exec_properties).