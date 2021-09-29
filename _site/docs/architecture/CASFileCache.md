---
layout: default
title: CASFileCache
parent: Architecture
nav_order: 6
---

The Content Addressable Storage File Cache (CFC) is the functional beating heart of all Buildfarm Workers. It should fundamentally be thought of as a rotating set of Content stored in files, indexed by Digests that make up the inputs for Actions to be executed. It also serves several other functions here detailed. This documentation presents it both in terms of functional behavior, as if it were a service or process, as well as with Java semantics, owing to its implementation.

A CFC has the additional responsibility in shard workers of presenting a CAS (Java) Interface, which supports the typical Read, Write, FindMissingBlobs, and batch versions of the R/W requests.

Each CFC needs a filesystem path to use as its root, and a set of configuration parameters. Under this path, it will store the CAS entries, named by their digests, optionally in a partitioned directory hierarchy by subdividing the first N bytes of the hash.

The CAS entries are treated by the CFC in terms of their index (filesystem directory) mapping to an inode (file content and limited metadata). This is an important distinction for the CFC, as it is used to present a substantially deduplicated ephemeral tree of inputs by its corresponding Exec Filesystem when executing actions. This will be described in further detail under the RC-LRU.

## File Metadata

Due to limitations of posix filesystem presentations, input FileNodes with 'executable' specifications require a unique inode from their non-executable counterparts. The CFC considers a CAS entry keyed with its path and executability uniquely, and their files are named as such: `<hash>` for the non-executable content, `<hash>_exec` for executable content. Each is charged with its own size to the storage pool, can be used for executable-idempotent requests on equal footing, and can be expired independently. Using any digest-based interface method without the option for an executable specification will result in executable key impartiality.

## Operations

### Read(digest, offset, limit): The CFC will locate the indexed file for a digest with executable impartiality, attempt to open it, seek to the offset requested, and present a limited stream of the content's bytes. A missing digest will result in predefined behavior per the CAS Interface, as well as an invalidation of its process index entry, if it exists. An access for the entry is inserted into the Access Queue. A read is (currently) impermeable to expiration, and can result in overcharge of filesystem content in such a case.

### Write(id): The CFC provides a Write-interface object that can be used to insert content into the CAS. Service and Operation lifetime implementations use this to inject content as requested, and leverage its restartable and asynchronous completion capacities. Writes may be rejected out of hand due to configurable CAS entry size limitations. Under the hood, there are several constraints present per-write:
* A Write is uniquely identified by its digest and a uuid.
* A Write will charge the storage pool size with its full content size upon opening a stream via getOutput().
* A Write's OutputStream is a mutually exclusive transaction, with a premature close() restoring the stream's capacity for representation.
* A close() upon completion of a Write's OutputStream will initiate a validation for it.
* Upon successful validation (size and content are compared to the expected digest), a commit is effected, making it available as a CAS entry with a recorded access placing it at the head of the RC-LRU.
* Within a CFC, concurrent Writes to a single digest will compete for completion, with the first one to commit cancelling all other Writes' OutputStreams to the same digest.
* Upon a commit, a shared future for all Writes to a single digest is completed, allowing premature indication of committed content receipt.

### FindMissingBlobs(digests): The process index is queried for each requested digest with executable impartiality, to filter for only those entries that do not exist in the index. No file interactions take place during this request, and no invalidation of content occurs upon this request. Accesses for all existing entries enter the Access Queue.

### Batch Read/Write: These requests are sequenced for each entry, resulting in only latency reduction, corresponding to each individual operation.

## Structures

### Entry Index

This is a Map of CFC Entry Keys ({digest, executability}) to accounting entry metadata. Reference counts, directories containing the entry, and file metadata ttl are metadata fields for the entry, as well as support fields for the RC-LRU.

### RC-LRU

The Reference Counted Least Recently Used list is a composite structure with the Process Index. Entries are either *in* or *out* of the RC-LRU, with only those entries *in* the RC-LRU considered for expiration. All entries with reference counts > 0 are *out* of the RC-LRU. Manipulation of the RC-LRU is synchronized for the entire CFC, with the actors being the transactive `put[Directory]()` and `decrementReferences()` methods, as well as the Access Recorder.

### Directory Index

Directories can be maintained by the CFC to provide several important heuristic optimizations, both in terms of storage and IO complexity. For a given directory entry, a key of its digest is used to refer to all of its flattened content digests. This is complemented by a filesystem directory named for the digest with a `<hash>_dir` name, where it will contain a) real directories underneath it named for each level of DirectoryNode in its REAPI specification, and b) files with names and executability from a FileNode for each Directory's files, the inodes for which are all CAS entry files (or empty files, in that special size case). The CFC also maintains a reverse index for each CAS entry to all of the directories it exists under. Upon expiration of an entry, each directory it is contained in is also expired, with both Directory Index and filesystem directories being deleted. This is the only growth bound check on any directory, being the only path to expiration.

### Access Queue

A simple queue of records of digest set accesses, meant to indicate the entries more desired than not represented.

## Agents

### Startup Scanner

Upon start(), a CFC scans the contents of its root to populate its indices. It first locates and injects all entry inodes, filtering for valid entry names and filesystem permissions, populating the Entry Index. Invalid names are deleted. It then scans directories, reproducing a Directory tree definition for each toplevel dir for validation, and populates the Directory Index. Invalid directories are deleted. A callback for the set of valid entry digests discovered is invoked. This procedure is (currently) blocking for valid entries, and operates asynchronously during invalid content removal.

### Access Recorder

The Access Queue is depleted by the Access Recorder, active after `start()` completion, to perform a simple reordering for accessed entries into the front of the RC-LRU. An entry that is *out* of the RC-LRU remains out after being processed by the recorder, and an entry that is *in* is moved to the head. A single thread runs the Access Recorder, and it is terminated when `stop()` is called on the CFC.

## Behaviors

### Reference/Dereference

Entries can have their reference counts incremented, with 0->1 moving them *out* of the RC-LRU, or decremented, with 1->0 moving them *in* to the RC-LRU, being placed at the head. Directory {,de}references operate in batch over their contained entries, with directory digests functioning as extreme space-saving shorthands for digest sets.

### Asynchronous Filesystem Monitor

Entry and Directory metadata content Time To Live checks provide convergent consistency in the event of asynchronous filesystem manipulation - in the event of an unexpected availability or permission change to the filesystem, the CFC eventually discovers and presents corrected state.

### Expiration

As a result of any intended insertion into the CFC, the total content size of the CAS is 'charged' by adding the size of the new content. A test is performed against this size. If the size exceeds a configurable maximum content size, expiration occurs and blocks the insertion until complete. To expire content, the CFC removes the tail of the RC-LRU, attempts a delegate write if configured, expires its associated directories, removes its metadata, deletes its file content, and will executable-impartially invoke a callback with the expired digest. This process is repeated until the size is once again below the maximum content size.

### Delegation

The CFC supports a waterfall delegation to a CAS Interface Object. This Object (currently) sees the following interactions:

* Writes upon expirations of a referencing CFC, with full output streaming.
* Reads upon content missing from a referencing CFC, with read through insertion of the referencing CFC as an entry
* FindMissingBlobs for filtered entries that remain missing from a referencing CFC.