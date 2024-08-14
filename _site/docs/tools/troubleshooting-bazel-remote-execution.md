---
layout: default
title: Troubleshooting
parent: Tools
nav_order: 1
---

Remote execution is sometimes an exercise fraught with problems, whether they be client definition, specification of remote endpoints, interactions with layered systems, and the complex operation of running a configured remote installation. This guide serves to give you some of the tools to debug your problems, from a bazel client point of view.

A typical use case: Something works locally, but breaks when remote execution is introduced. Beyond stdout, and execution instrumentation, you will probably want to find out what step along the path to remote operation responses.

## bazel logging

Use `bazel [build|run|test] --remote_grpc_log=<filename>` (`--experimental_remote_grpc_log=<filename>` if you are using bazel older than 6.0 release) to produce a binary log of all of the grpc activity bazel performs during an invocation. This log is written to at the completion of each request, and may not contain a complete picture if a build is interrupted, or a request is currently ongoing.

## Dumping the log

Use [tools_remote](https://github.com/bazelbuild/tools_remote). Build the remote_client in it with `bazel build :remote_client`. Since we're going to point to a local file, it might be easiest to use the `bazel-bin/remote_client` executable to perform a text dump of the previously generated bazel log file:

`$ bazel-bin/remote_client --grpc_log=<filename> printlog`

Warning: the original log file may be *big*, and the corresponding text output may be similarly *big*. This text output is invaluable, however, when trying to associate or distinguish the activities of a single build, and we strongly recommend that users report bugs with these logs attached, in either form.

## The log file

The log file retains all of the Calls (i.e. Requests and Responses) for (at least) content related to remote execution. These calls will have either no status code response (0, OK, with the default proto value for ints omitted), or a numeric status code. Use https://github.com/grpc/grpc/blob/main/doc/statuscodes.md to reference these until tools_remote can print a nicer representation. Also included in each call is the timestamp of the call, its final response, and for streaming calls, some information about how the call proceeded through retries and progressive activity. There is also RequestMetadata information printed for each request to indicate which correlated build id, build id, and action a call is associated with. The action is consistent across all of the requests for a single action, and is vital to group the disparate calls within a log to the same action context. It also helps to establish an expected flow of the sequence of calls.

You will see some (or none) of the following Calls in this log file:

**Capabilities::GetCapabilities** - asking the remote service what is supported.
**ActionCache::GetActionResult** - a call to the action cache service to retrieve an action result. This takes a 'digest key', a hex number whose size depends on your hash policy, likely the default SHA256, packaged with the size of the original content, looking something like `a948904f2f0f479b8f8197694b30184b0d2ed1c1cd2a1ec0fb85d299a192a447/42`. The responses, if successful, will contain the results of an action execution, and constitute a cache HIT. A NOT_FOUND status response corresponds to a cache MISS.
**ByteStream::Read** - Each one of these is a download for a digest that, very likely, was reported in an action result. These likewise use digests in their resource_names, and if you correspond the digest in a Read to a digest in an Action Result, you will learn the name of a file being downloaded.
**ContentAddressableStorage::FindMissingBlob** - The client is likely asking the remote system which of a set of digests does NOT exist in the CAS. An empty response means that everything is present remotely already. Any blobs returned will need to be uploaded by the client. You can find the action key in this set, at a minimum. This call occurs before any uploads of content, because it drives those calls, within an action's context.
**ByteStream::Write** - Each of these is an upload for a digest that, very likely, was returned in a FindMissingBlobs response. Digests are in their resource_names again, with an additional `uuid` to indicate a particular client's upload of a file.
Execution Execute - this is the call that directs the remote execution system to execute an action. Each response will be listed as the execution moves through one of several states on the remote side, likely culminating in COMPLETED. Note that this does not mean a *successful* execution, only that an operation is done executing, and now has its exit status, and outputs, if any, available for download in the CAS.
