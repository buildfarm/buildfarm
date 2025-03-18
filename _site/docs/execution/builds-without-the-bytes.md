---
layout: default
title: Build Without Bytes
parent: Execution
nav_order: 4
---

# Builds Without The Bytes

**TL;DR**: [BwoB](https://bazel.build/reference/command-line-reference#flag--remote_download_outputs) is supported by Buildfarm. Build with `--experimental_remote_cache_eviction_retries=5`.

As proposed in this [issue](https://github.com/bazelbuild/bazel/issues/6862) and the accompanying document, bazel endeavors to provide a mechanism to be 'content-light' for remote execution, using only content reference addresses to request action execution and construct successively dependent action definitions.

Builds Without The Bytes skips the download of action output files until it needs them.

Bazel now (>= 7) also applies a wall-clock ttl to every action result it downloads, depending upon current settings. See bazel documentation for the default.

If this ttl has not expired, regardless of bazel daemon uptime, the remote cache will not be queried for an action result.

The stored ActionResult also has no source - if you switch bazel `--remote_cache` or `--remote_executor` targets inside of the ttl, bazel will blindly assume the current target has ActionResult contents.

If ActionResult contents expire on Buildfarm, when bazel requests them, it will fail, hopefully with the special REMOTE_CACHE_EVICTED (39) exit code. The [flag](https://bazel.build/reference/command-line-reference#flag--experimental_remote_cache_eviction_retries) `--experimental_remote_cache_eviction_retries` will cause bazel to restart a build _with no intervention required_, for the specified limit # of times.

Buildfarm (since [2.11](https://github.com/buildfarm/buildfarm/releases/tag/2.11.0)) defaults to `ensureOutputsPresent: true` in server configs. When bazel requests an ActionResult, it will be NOT_FOUND (cache miss) unless all of its contents exist in the CAS. This also extends the lifetime of those contents (without shard loss) to the minimum ttl before expiration.

With `ensureOutputsPresent: false`, there is also a mechanism to control this check on a per-request level.

The REAPI presents a 'correlated_invocations_id' in the RequestMetadata settable on headers for every request to BuildFarm, including the GetActionResult request, which it uses to retrieve cached results. BuildFarm recognizes this correlated_invocations_id and if it is a URI, can parse its query parameters for behavior control. One such control is ENSURE_OUTPUTS_PRESENT for the GetActionResult request - if this query value is the string "true", BuildFarm will make a silent FindMissingBlobs check for all of the outputs of an ActionResult before responding with it. If any are missing, BuildFarm will instead return code NOT_FOUND, inspiring the client to see a cache miss, and attempt a [remote] execution.
