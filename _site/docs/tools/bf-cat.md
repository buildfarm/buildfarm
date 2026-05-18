---
layout: default
title: bf-cat
parent: Tools
nav_order: 2
---

bf-cat is a tool provided with buildfarm for requesting, retrieving, and displaying information related to REAPI services.

Build it with `bazel build //src/main/java/build/buildfarm/tools:bf-cat`

## Usage

```
bf-cat <host[:port]> <instance-name> <subcommand> [params...]
```

Use `--help` on bf-cat or any subcommand to see available options:

```
bf-cat --help
bf-cat <host[:port]> <instance-name> Action --help
```

**host** is the `[scheme://]host:port` of the buildfarm server. Scheme should be `grpc://`, `grpcs://`, or omitted (default `grpc://`).

**instance-name** is the name of the specific instance to inquire about, typically configured on schedulers. A literal empty string parameter (i.e. bash: `""`) will use the default instance for a server.

## Subcommands

Digest parameters are specified as `<hash>/<size>`, as typically represented in log entries.

### Blob & CAS Inspection

* **`Action <digest>...`**: Retrieves Action definitions from the CAS and renders them with field identifiers.
* **`ActionResult <digest>...`**: Get results of an Action from the ActionCache.
* **`Command <digest>...`**: Retrieves Command definitions from the CAS and renders them with field identifiers.
* **`Directory <digest>...`**: Retrieves Directory definitions from the CAS and renders them with field identifiers.
* **`File <digest>...`**: Downloads a Blob from the CAS and prints it to stdout (60s time limit). This can be safely redirected to a file, with no additional output interceding.
* **`Missing <digest>...`**: Make a findMissingBlobs request, outputting only the digests in the parameter list that are missing from the CAS.
* **`QueuedOperation <digest>...`**: Retrieves the definition of a prepared operation for execution.
* **`DumpQueuedOperation <digest>...`**: Binary QueuedOperation content, suitable for retention in a local `blobs` directory and use with `bf-executor`.

### Tree Inspection

* **`DirectoryTree <digest>...`**: Simple recursive root Directory listing; missing directories will error.
* **`TreeLayout <digest>...`**: Rich tree layout of a root directory with weighting and missing tolerance. A Tree is printed with indent-levels according to depth in the directory hierarchy with FileNode and DirectoryNode fields with digests for each entry, as well as a weight by byte and % of the sizes of each directory subtree.
* **`REDirectoryTree <digest>...`**: Vanilla REAPI Tree description (NOT buildfarm Tree with index).
* **`RETreeLayout <digest>...`**: Vanilla REAPI Tree with rich weighting like TreeLayout.

### Operations

* **`Operation <name>...`**: Retrieves current operation statuses and renders them with field identifiers. This uses the Operations API and will include rich information about operations in flight.
* **`Operations [filter] [name]`**: Retrieves a list of operations pertaining to a filter and a name scope:
  * `toolInvocationId=<uuid>`: list executions in a client invocation group
  * `correlatedInvocationsId=<uuid> toolInvocations`: list the invocations in a client correlated list
  * `status=dispatched`: list the currently dispatched executions
* **`Watch <name>...`**: Watch operations to retrieve status updates about their progress through the operation pipeline until completed.

### Server & Worker Status

* **`Capabilities`**: List remote capabilities for an instance.
* **`BackplaneStatus`**: Retrieve the status of a shard cluster's operation queues, with discrete information about each provisioned layer of the ready-to-run queue.
* **`WorkerProfile [names...]`**: Retrieve profile information about workers' operations, including the size of the CAS and the relative performance of the execution pipeline.

### Miscellaneous

* **`Fetch <uris>...`**: Request a URI fetch via the assets API.
* **`WriteStatus <resourceNames>...`**: Retrieve write status for upload resources.
