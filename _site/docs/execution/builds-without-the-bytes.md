---
layout: default
title: Build Without Bytes
parent: Execution
nav_order: 4
---

# Builds Without The Bytes

tl;dr: add `--build_request_id=https://host?ENSURE_OUTPUTS_PRESENT=true#$(uuidgen)`, to your BWOB bazel invocations, or enable `ensureOutputsPresent` in your config to set it globally.

As proposed in this [issue](https://github.com/bazelbuild/bazel/issues/6862) and the accompanying document, bazel endeavors to provide a mechanism to be 'content-light' for remote execution, using only content reference addresses to request action execution and construct successively dependent action definitions.

Simply put, Builds Without The Bytes skips the download of action result artifacts until it needs them.

This puts BuildFarm in the uncomfortable position of never being able to expire any unique content associated with ActionResults, which is not something easily accomplished. It extends the required lifetime to some duration where bazel retains references, and if it encounters a situation where it either wants them, or to execute an action that depends upon them, and they are not available, it fails.

To combat this, you can provide some metadata to buildfarm that will help to limit (but will not remove the possibility of) failed builds.

Bazel presents a 'correlated_invocations_id' on every request to BuildFarm, including the GetActionResult request, which it uses to retrieve cached results. Since ActionResults are the long tail survivor of actions, being retained for much longer after one executes and produces its content, this represents the most likely position where content may have been removed, and a stale reference might be provided. BuildFarm recognizes this correlated_invocations_id and if it is a URI, can parse its query parameters for behavior control. One such control is ENSURE_OUTPUTS_PRESENT for the GetActionResult request - if this query value is the string "true", BuildFarm will make a silent FindMissingBlobs check for all of the outputs of an ActionResult before responding with it. If any are missing, BuildFarm will instead return code NOT_FOUND, inspiring the client to see a cache miss, and attempt a [remote] execution.
