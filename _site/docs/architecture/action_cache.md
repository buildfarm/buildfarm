---
layout: default
title: Action Cache
parent: Architecture
nav_order: 5
---

ActionCache is a service that can be used to query whether a defined action has already been executed and, if so, download its result. The service API is defined in the [Remote Execution API](https://github.com/bazelbuild/remote-apis). `ActionCache` service would require [`ContentAddressableStorage`](https://github.com/buildfarm/buildfarm/wiki/ContentAddressableStorage) service to store file data.

An `Action` encapsulates all the information required to execute an action. Such information includes the command, input tree containing subdirectory/file tree, environment variables, platform information. All the information will contribute to the digest computation of an `Action` so that execution of an `Action` multiple times will produce the same output. With this, hash of an `Action` can be used as a key to cached `ActionResult`, which store result and output of an `Action` after an `Action` is completed. `ActionResult`s can be populated in `ActionCache` service after `Action`s get completed by an `Execution` service. They can also come from a local Bazel client that has executed the `Action`s and put the `ActionResults` into the cache by using the `UpdateActionCache` method. In other words, The `ActionCache` service can be used without using/implementing the `Execution` service.

By leveraging `Action` definition, `ActionCache` service is responsible for mapping `Action`s to the `ActionResult`s.

# Methods

## GetActionResult
Essentially the "get" method, which is responsible for finding an ActionResult and retrieving it. Before invoking this method, Bazel client should compute the input tree and the `Action` message for the action needs to be done. Then Bazel can use this `GetActionCache` method to see if the action has been completed successfully and, if so, use `bytestream.Read` to download the outputs.
## UpdateActionResult
As mentioned above, the `ActionCache` service doesn't necessarily need an `Execution` Service. In this case, a "put" method is required so that an `ActionResult` can be directly put into the cache. This is what `UpdateActionResult` is designed for. With this method, Bazel clients running different machines can upload their build results into a cache pool, which can be available to other users through `GetActionResult`.

# Buildfarm Implementations

Buildfarm provides implementations of both `ActionCache` and `Execution`. `ActionResult` can be populated through `Execution` service or uploaded by local Bazel clients through `UpdateActionCache`.

The `ActionCache` service is hosted on the server-side of Buildfarm. When a `GetActionResultRequest` received through `GetActionResult` call, by using the `instance_name` field of the request, the `ActionCache` service will find the `Instance` that is supposed to have `ActionResult`, and it will find the `Instance` and get the `ActionResult` asynchronously. Similarly, an `UpdateActionResultRequest` will be sent to the `ActionCache` service through `UpdateActionResult` rpc call. The service will find the `Instance` that is supposed to have the `ActionResult` through the `instance_name` field of the request and update the corresponding instance.


