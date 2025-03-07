---
layout: default
title: bf-cat
parent: Tools
nav_order: 2
---

bf-cat is a tool provided with buildfarm for investigating the various structures and status of your Buildfarm Cluster.

Build or run it with `bazel build //src/main/java/build/buildfarm/tools:bf-cat`

Its basic usage is:

`bf-cat <host[:port]> <instance-name> <command> [params...]`

**instance-name** is the name of the specific instance to inquire about, typically configured on schedulers. A literal empty string parameter (i.e. bash: `""`) will use the default instance for a server.

**command** is typically one of the following, with digest parameters as <hash>/<size>, as typically represented in log entries:

* **`Action <digest>`**: Retrieves Action definitions from the CAS and renders them with field identifiers.
* **`Capabilities`**: Retrieve the capabilities response for an instance.
* **`Command <digest>`**: Retrieves Command definitions from the CAS and renders them with field identifiers.
* **`Directory <digest>`**: Retrieves Directory definitions from the CAS and renders them with field identifiers.
* **`Fetch <uris>...`**: Request an uri fetch via the assets API.
* **`File <digest>`**: Downloads a Blob from the CAS and prints it to stdout. This can be safely redirected to a file, with no additional output interceding
* **`Missing <digest>...`**: Make a findMissingBlobs request, outputting only the digests in the parameter list that are missing from the CAS
* **`Operation <name>...`**: Retrieves current operation statuses and renders them with field identifiers as able. This uses the Operations API and will include rich information about operations in flight, compared to the 'execute' function
* **`BackplaneStatus`**: Retrieve the status of a shard cluster's operation queues, with discrete information about each provisioned layer of the ready-to-run queue.
* **`TreeLayout <digest>`**: Retrieves Trees of inputs from a root node. A Tree is printed with indent-levels according to depth in the directory hierarchy with FileNode and DirectoryNode fields with digests for each entry, as well as a weight by byte and % of the sizes of each directory subtree.
* **`WorkerProfile`**: Retrieve profile information about a worker's operation, including the size of the CAS and the relative performance of the execution pipeline
* **`Watch <name>`**: Watch an operation to retrieve status updates about its progress through the operation pipeline
