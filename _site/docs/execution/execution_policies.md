---
layout: default
title: Policies
parent: Execution
nav_order: 5
---

Execution Policies can be defined to modify or control Action execution on workers. A given policy can be assigned a name, with a policy with a default name (the empty string) affecting all executions on a worker, as well as a single type of modifier.

Policies are applied in order of definition for a single matching name, with the default policies applied first, followed by the order specified effective by the action.

## Wrapper execution policy modifier type

This policy type specifies that a worker should prepend a single path, and a number of arguments, to the execution of a subprocess to generate an action result. These arguments have a limited substitution mechanism that discovers any appearance of `<property-name>` and substitutes it with a string representation of a value currently available in the platform properties for the action. _If a specified `property-name` platform property is not present for the action, the wrapper is discarded entirely._ Note that this substitution does not apply to the `path` of the wrapper, which may point to any file with appropriate permissions - executable, and readable if necessary as a shell script, on linux, for example.

### Example:

This example will use the buildfarm-provided executable `as-nobody`, which will upon execution demote itself to a `nobody` effective process owner uid, and perform an `execvp(2)` with the remaining provided program arguments, which will subsequently execute as a user that no longer matches the worker process.

```yaml
# default wrapper policy application
worker:
  executionPolicies:
    - name: test
      executionWrapper:
        path: "/app/buildfarm/as-nobody"
```

## Action Specification

An execution may be requested for an Action definition which includes the platform property `execution-policy`, which will select from the available execution policies present on a worker. A worker will not execute any action for which it does not have definitions matching its requested policies, similar to other platform requirements.

## Built-in Execution Policies
Buildfarm images are packaged with custom execution wrappers to be used as policies.  Some of these wrappers are chosen dynamically based on the action.  For example, the bazel-sandbox is included with buildfarm and can be chosen with `exec_property{"linux-sandbox": "True"}`.  Below is a description of the execution wrappers provided:

### process-wrapper
The process wrapper also used by bazel to allow Java to reliably kill processes.  It is recommended this is used on all actions.

### linux-sandbox
The sandbox bazel uses when running actions client-side.  This provides many isolations for actions such as tmpfs and block-network.  It may also have performance issues.  We include it with buildfarm to ensure additional consistency between local/remote actions.

### tini
tini is a [a tiny but valid init for containers](https://github.com/krallin/tini).  Depending on how buildfarm is containerized you may want to use.

### as-nobody
This is used to set the action's user to "nobody".  Otherwise buildfarm will run the action as root which may be undesirable.

### skip_sleep (skip_sleep.preload + delay)
These wrappers are used for detecting actions that rely on time.  Below is a demonstration of how they can be used.
This addresses two problems in regards to an action's dependence on time.  The 1st problem is when an action takes longer than it should because it's sleeping unnecessarily.  The 2nd problem is when an action relies on time which causes it to eventually be broken on master despite the code not changing.  Both problems are expressed below as unit tests.  We demonstrate a time-spoofing mechanism (the re-writing of syscalls) which allows us to detect these problems generically over any action.  The objective is to analyze builds for performance inefficiency and discover future instabilities before they occur.

### Issue 1 (slow test)

```bash
#!/usr/bin/env bash
set -euo pipefail

echo -n "testing... "
sleep 10;
echo "done"
```

The test takes 10 seconds to run on average.

```shell
$ bazel test --runs_per_test=10 --config=remote //cloud/buildfarm:sleep_test
//cloud/buildfarm:sleep_test                                             PASSED in 10.2s
  Stats over 10 runs: max = 10.2s, min = 10.1s, avg = 10.2s, dev = 0.0s
```

We can check for performance improvements by using the `skip-sleep` option.

```shell
$ bazel test --runs_per_test=10 --config=remote --remote_default_exec_properties='skip-sleep=true' //cloud/buildfarm:sleep_test
//cloud/buildfarm:sleep_test                                             PASSED in 1.0s
  Stats over 10 runs: max = 1.0s, min = 0.9s, avg = 1.0s, dev = 0.0s
```

Now the test is 10x faster.  If skipping sleep makes an action perform significantly faster without affecting its success rate, that would warrant further investigation into the action's implementation.

### Issue 2 (future failing test)

```bash
#!/usr/bin/env bash
set -euo pipefail

CURRENT_YEAR=$(date +"%Y")
if [[ "$CURRENT_YEAR" -eq "2021" ]]; then
        echo "The year matches."
        date
        exit 0;
fi;
echo "Times change."
date
exit -1;
```

The test passes today, but will it pass tomorrow?  Will it pass a year from now?  We can find out by using the `time-shift` option.

```shell
$ bazel test --test_output=streamed --remote_default_exec_properties='time-shift=31556952' --config=remote //cloud/buildfarm:future_fail
INFO: Found 1 test target...
Times change.
Mon Sep 25 18:31:09 UTC 2023
//cloud/buildfarm:future_fail                                            FAILED in 18.0s
```

Time is shifted to the year 2023 and the test now fails.  We can fix the problem before others see it.
