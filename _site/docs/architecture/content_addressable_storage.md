---
layout: default
title: CAS
parent: Architecture
nav_order: 6
---


A Content Addressable Storage (CAS) is a collection of service endpoints which provide read and creation access to immutable binary large objects (blobs). The core service is declared in the [Remote Execution API](https://github.com/bazelbuild/remote-apis), and also requires presentation of the [ByteStream API](https://github.com/googleapis/googleapis/blob/master/google/bytestream/bytestream.proto) with specializations for resource names and behaviors.

An entry in the CAS is a sequence of bytes whose computed digest via a hashing function constitutes its address. The address is specified as either a [Digest] message, or by the makeup of a resource name in ByteStream requests.

A CAS supports several methods of retrieving and inserting entries, as well as some utility methods for determining presence and iterating through linked hierarchies of directories as Merkle Trees.

Functionally within the REAPI, the CAS is the communication plane for Action inputs and outputs, and is used to retain other contents by existing clients, including bazel, like build event information.

# Methods

## Reads

A read of content from a CAS is a relatively simple procedure, whether accessed through BatchReadBlobs, or the ByteStream Read method. The semantics associated with these requests require the support of content availability translation (NOT_FOUND for missing), and seeking to a particular offset to start reading. Clients are expected to behave progressively, since no size limitation nor bandwidth availability is mandated, meaning that they should advance an offset along the length of the content until complete with successive requests, assuming DEADLINE_EXCEEDED or other transient errors occur during the download. `resource_name` for reads within ByteStream Read must be `"{instance_name}/blobs/{hash}/{size}"`

## Writes

Writes of content into a CAS require a prior computation of an address with a digest method of choice for all content. A write can be initiated with BatchUpdateBlobs or the ByteStream Write method. A ByteStream Write `resource_name` must begin with `{instance_name}/uploads/{uuid}/blobs/{hash}/{size}`, and may have any trailing filename after the size, separated by '/'. The trailing content is ignored. The `uuid` is a client generated identifier for a given write, and may be shared among many digests, but should be strictly client-local. Writes should respect a WriteResponse received at any time after initiating the request of the size of the blob, to indicate that no further WriteRequests are necessary. Writes which fail prior to the receipt of content should be progressive, checking for the offset to resume an upload via ByteStream QueryWriteStatus.

Buildfarm implements the CAS in a variety of ways, including an in-memory storage for the reference implementation, as a proxy for an external CAS, an HTTP/1 proxy based on the remote-cache implementation in bazel, and as a persistent on-disk storage for workers, supplementing an execution filesystem for actions as well as participating in a sparsely-sharded distributed store.

# Buildfarm Implementations

Since these implementations vary in complexity and storage semantics, a common interface was declared within Buildfarm to accommodate substitutions of a CAS, as well as standardize its use. The specifics of these CAS implementations are detailed here.

## Memory

The memory CAS implementation is extremely simple in design, constituting a maximum size with LRU eviction policy. Entry eviction is a registrable event for use as a storage for the delegated ActionCache, and Writes may be completed asynchronously by concurrent independent upload completion of an entry.

This is the example presentation of a CAS in the memory instance available [here](https://github.com/bazelbuild/bazel-buildfarm/blob/85c3fdaf89fedc2faee1172fab01338777de79a1/examples/server.config.example#L40-L45), but for reference, specification in any `cas_config` field for server or worker will enable the creation of a unique instance.

```
cas_config: {
  memory: {
    # limit for CAS total content size in bytes
    max_size_bytes: 1073741824 # 1024 * 1024 * 1024
  }
}
```

## GRPC

This is a CAS which completely mirrors a target CAS for all requests, useful as a proxy to be embedded in a full Instance declaration.

A grpc config example is available in the alternate instance specification in the memory server example [here](https://github.com/bazelbuild/bazel-buildfarm/blob/85c3fdaf89fedc2faee1172fab01338777de79a1/examples/server.config.example#L91-L99). For reference:

```
cas_config: {
  grpc: {
    # instance name for CAS resources, default is empty
    instance_name: "internal"

    # target suitable for netty channel
    target: "cas-host.cloud.org:5000"
  }
}
```

## HTTP/1

The HTTP/1 CAS proxy hosts a GRPC service definition for a configurable target HTTP/1 service that it communicates with using an identical implementation to the [bazel http remote cache protocol](https://github.com/bazelbuild/bazel/tree/master/src/main/java/com/google/devtools/build/lib/remote/http).

Since the HTTP/1 proxy is a separate process, there are no configuration options for it. Instead, run the proxy in a known location (address and port), and use a grpc configuration indicated above, pointing to its address and instance name. The proxy can be run with:

`bazel run //src/main/java/build/buildfarm:buildfarm-http-proxy -- -p 8081 -c "http://your-http-endpoint"`

And will result in a listening grpc service on port 8081 on all interfaces, relaying requests to the endpoint in question. Use `--help` to see more options.

## Shard

A sharded CAS leverages multiple Worker CAS retention and proxies requests to hosts with isolated CAS shards. These shards register their participation and entry presentation on a ShardBackplane. The backplane maintains a mapping of addresses to the nodes which host them. The sharded CAS is an aggregated proxy for its members, performing each function with fallback as appropriate; FindMissingBlobs requests are cycled through the shards, reducing a list of missing entries, Writes select a target node at random, Reads attempt a request on each advertised shard for an entry with failover on NOT_FOUND or transient grpc error. Reads are optimistic, given that a blob would not be requested that was not expected to be found, the sharded CAS will failover on complete absence of a blob to a whole cluster search for an entry.

A shard CAS is the default for the Shard Instance type, with its required [backplane specification](https://github.com/bazelbuild/bazel-buildfarm/blob/85c3fdaf89fedc2faee1172fab01338777de79a1/examples/shard-server.config.example#L17-L239). Since functionality between Shard CAS, AC, and Execution are mixed in here, the definition is somewhat cluttered, with efforts to refine specific aspects of it underway.

## Worker CAS

Working hand in hand with the Shard CAS implementation, the Worker CAS leverages a requisite on-disk store to provide a CAS from its CASFileCache. Since the worker maintains a large cache of inputs for use with actions, this CAS is routinely populated from downloads due to operation input fetches in addition to uploads from the Shard frontend.

### CasFileCache
The worker's CAS file cache uses persistent disk storage. A strongly recommended filesystem to back this is XFS, due to its high link counts limits per inode. A strongly discouraged filesystem is ext4, which places a hard limit of 65000 link counts per inode.  The layout of the files are ordered such that file content, in the form of canonical digest filenames for inode storage, remains on the root of the cache directory, while exec roots and symlinkable directories contain hard links to these inodes. This avoids unnecessary duplication of file contents.

Upon worker startup, the worker's cache instance is initialized in two phases. First, the root is scanned to store file information. Second, the existing directories are traversed to compute their validating identification.  Files will be automatically deleted if their file names are invalid for the cache, or if the configured cache size has been exceeded by previous files.

Each Worker type's specification of the CASFileCache is unique. The memory worker defaults to CASFileCache with no option to change it, only configure its [root directory](https://github.com/bazelbuild/bazel-buildfarm/blob/85c3fdaf89fedc2faee1172fab01338777de79a1/examples/worker.config.example#L46-L47) and [configuration options](https://github.com/bazelbuild/bazel-buildfarm/blob/85c3fdaf89fedc2faee1172fab01338777de79a1/examples/worker.config.example#L111-L117). The shard worker allows a more flexible specification, with delegates available of the other types to fall back on for CAS expansion, and [encapsulated CAS configs](https://github.com/bazelbuild/bazel-buildfarm/blob/85c3fdaf89fedc2faee1172fab01338777de79a1/examples/shard-worker.config.example#L49-L65)

The CASFileCache is also available on MemoryInstance servers, where it can represent a persistent file storage for a CAS. An example configuration for this is:

```
cas_config: {
  filesystem: {
    # the local cache location relative to the 'root', or absolute
    path: "cache"


    # limit for contents of files retained
    # from CAS in the cache
    max_size_bytes: 2147483648 # 2 * 1024 * 1024 * 1024


    # limit for content size of files retained
    # from CAS in the cache
    max_entry_size_bytes: 2147483648 # 2 * 1024 * 1024 * 1024
  }
}
```

CASTest is a standalone tool to load the cache and print status information about it.