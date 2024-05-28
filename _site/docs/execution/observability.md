---
layout: default
title: Observability
parent: Execution
nav_order: 7
---

# Observability

Executions are organized by a hierarchy of ownership, and indexed with client-specified fields. The [longrunning.operations](https://cloud.google.com/service-infrastructure/docs/service-management/reference/rpc/google.longrunning) API can be used to find a configuration-limited number of executions organized in this hierarchy by `name` and `filter`.

## Hierarchy

 * Correlated Invocations - specified by build tool invoker
   * Tool Invocation - generated internally by build tool
     * Execution - generated internally by buildfarm

An *Execution* is an individual Execute request made from a client to buildfarm. The names for all Executions on buildfarm will be the string `<instance name>/executions/<uuid>`, with `uuid` being generated after the Execute call is made.

A *Tool Invocation* is an organization of all requests made by a client during one logical user invocation. For instance, `bazel`, as a _tool_, will generate a UUID per _invocation_, usually by a user or CI script - e.g: each time you run `bazel`, it generates internally and uses a different `toolInvocationId`, making this a collection of _Executions_ (and other requests) per run.

A set of *Correlated Invocations* is uniquely identified by another UUID, the `correlatedInvocationsId`, which is expected to encompass one or more _Tool Invocations_. A build tool will provide the mechanism to specify this by a user - e.g: each time you run `bazel`, you can specify the same `--build_request_id` to indicate that a run is a member of a collection of _Tool Invocations_. The correlatedInvocationsId must be a string that _ends_ with an 8-4-4-4-12 format UUID string (important because this value is completely specified by a client user), otherwise it is ignored and buildfarm will emit a warning.

## Indexing

Note that the `correlatedInvocationsId` only needs to _end_ in a UUID. This means that before the end of the id, this can contain whatever identifying information we want.

Buildfarm has taken steps that make it beneficial to provide this information as a URI. The `fragment` of the URI is an excellent place to specify the required trailing UUID. For example, our archetypal builder "Alice" provided this:

```
https://alice@dev-workstation/projects/winthorp/modules/interpreter?org=database&milestone=epsilon#fda159f2-bafe-4270-91ba-c7a946c0f2ba
```

She's specified a userinfo containing her `username` (the `password` field of `userinfo` will be ignored by buildfarm), the `host` the build ran on, a path that identified her `project` and its `module` per her organization, which is listed here as `database`, and she's clearly working on the 'epsilon' `milestone`. Trailing it after a fragment separator is the required uuid.

A single layer of optional indexing is provided in buildfarm to collect _Correlated Invocations_ with useful identifying information.

The indexing starts with the configuration of buildfarm's server:

```yaml
server:
  ...
  correlatedInvocationsIndexScopes: !!set
    ? host
    ? username
```

This means that the `host` and `username` fields should be indexed for correlatedInvocationsIds observed. This configuration is the default, and you may remove or add any indexing you wish.

These values are extracted from the correlatedInvocationsId on every request, and must not differ in content contained within the URI without also changing the UUID, in order to guarantee proper indexing.
Any values other than the above will be extracted from the query of the URI - `host` will come from the component in `authority`, and `username` from `userinfo`.

These will create indices which identify `correlatedInvocations` via the UUID portion of the correlatedInvocationsId, making this a series of collections of _correlatedInvocations_

## listOperations

The Longrunning Operations method `listOperations` will serve as the entry point to discover elements of this hierarchy.

In the interest of a speedy response, the Operations it returns will not have any details other than `name` populated. More detailed information about a name returned can be queried with `getOperation`. The `listOperations` response can then be considered a list of names for these examples.

The ListOperationsRequest `name` and `filter` fields have several special meanings intended for discovery. The instance name listed here will be `solomon` for identification purposes.

```
ListOperationsRequest({
  name: "solomon",
})
```

Will yield the names of the indices and special bindings. In the case of the above, after running a build with the above `correlatedInvocationsId` and configuration:

```
executions
toolInvocations
correlatedInvocations
host
username
```

To discover the available `correlatedInvocations` index entries for `username`:

```
ListOperationsRequest({
  name: "solomon/username",
})
```

Which will yield:

```
username=alice
username=bob
username=mallory
```

For a system where usernames `alice`, `bob`, and `mallory` have been presented in `correlatedInvocationsId` URIs.

To see the `correlatedInvocationsIds` for `mallory`, we can switch to a filter and select the `correlatedInvocations` parent resource:

```
ListOperationsRequest({
  name: "solomon/correlatedInvocations",
  filter: "username=mallory",
})
```

Note that the format is the same as the one in the output of the index entries result. This yields:

```
0c1ce9e6-471d-4265-b15b-885e42670fdb
7bd7f8d2-6628-4f3f-a7e3-9eff917eb697
6ca27615-4fb5-4ffc-9349-d484926bd05e
```

So `username=mallory` is associated with these three `correlatedInvocations`. These are `correlatedInvocationsId`s which can be passed to the `toolInvocations` parent resource:

```
ListOperationsRequest({
  name: "solomon/toolInvocations",
  filter: "correlatedInvocationId=0c1ce9e6-471d-4265-b15b-885e42670fdb",
})
```

To get a list of `toolInvocationId`s:

```
fad33d5e-fbba-483d-8df4-4502621caff2
44e516cd-d8fd-4c61-90d2-9389402242a4
```

This `correlatedInvocationId` was used in two client invocations. Our last stop on the hierarchy is to pass this to the `executions` binding associated with its `toolInvocationId`:

```
ListOperationsRequest({
  name: "solomon/executions",
  filter: "toolInvocationId=fad33d5e-fbba-483d-8df4-4502621caff2",
})
```

This yields a huge list of the names of executions that the `toolInvocation` `fad33d5e-fbba-483d-8df4-4502621caff2` requested via the `Execute` method.

The Operation-encapsulated information from these execution UUID results can be retrieved via `getOperation` with a name of `solomon/executions/<uuid>`. Those executions will have a wealth of information about where and how they executed, and will be up to date with the current reporting if the execution is still in flight.

## Tools

bf-cat can issue all of the example requests listed here to both `listOperations` and `getOperation`:

```
bf-cat ... Operations <filter> <name-without-index>
```

for `listOperations`, and

```
bf-cat ... Operation <operation-name> [<more-operation-names>...]
```

for `getOperation`, which supports requesting multiple uuids in a single invocation.
