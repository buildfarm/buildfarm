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
