# Buildfarm

[![Build status](https://badge.buildkite.com/45f4fd4c0cfb95f7705156a4119641c6d5d6c310452d6e65a4.svg?branch=main)](https://buildkite.com/buildfarm/buildfarm)
[![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/buildfarm/buildfarm/badge)](https://securityscorecards.dev/viewer/?uri=github.com/buildfarm/buildfarm)
![License](https://img.shields.io/github/license/buildfarm/buildfarm)
![Latest release](https://img.shields.io/github/v/release/buildfarm/buildfarm)
![Docker pulls](https://img.shields.io/docker/pulls/bazelbuild/buildfarm-server)

**Buildfarm is an open-source remote cache and remote execution service.** It implements the [Bazel Remote Execution API][remote-apis] and can serve Bazel, Buck2, Pants, and other compatible clients.

Buildfarm combines a distributed content-addressable store (CAS), action cache, scheduler, and execution workers. Run it locally for development, or deploy a fleet of specialized workers for shared CI and developer builds.

| Get started | Learn | Get help |
| --- | --- | --- |
| [Quick start][quick-start] | [Documentation][docs] | [GitHub issues][issues] |
| [Helm chart](#deploy-with-helm) | [Configuration][configuration] | [Build Team Slack][slack] `#buildfarm` |

> **Looking for background on Bazel remote caching and execution?** See the [Bazel documentation][bazel-remote].

## Get started

Follow the [Quick Start guide][quick-start] to run a local Redis-backed Buildfarm server and worker, configure Bazel for remote caching or execution, and verify the result. It is the maintained source for local setup instructions.

For deployment design and operations, see the [configuration reference][configuration], [architecture guides][architecture], [worker execution environment guide][worker-environment], and [observability documentation][observability].

## Remote APIs compatibility

Buildfarm is built against Remote APIs commit [`6def1c5d27a527c400875c24ae8b1a160145d7e1`][remote-apis-pin]. The table below covers optional services, RPCs, and negotiated capabilities in that API family, plus the newer upstream replacements for deprecated chunking RPCs. It describes the full Buildfarm server; a deployment can intentionally disable execution, CAS participation, Action Cache writes, or absolute symlink targets through configuration.

| Area              | Optional API feature                                                                            | Supported | Notes                                                                                                            |
|-------------------|-------------------------------------------------------------------------------------------------|:---------:|------------------------------------------------------------------------------------------------------------------|
| Core RE           | Action Cache `UpdateActionResult`                                                               |    ✅     | Can be made read-only with `server.actionCacheReadOnly`.                                                         |
| Core RE           | Execution priorities                                                                            |    ✅     | Advertises the full signed 32-bit priority range.                                                                |
| Core RE           | `Execute.skip_cache_lookup`                                                                     |    ✅     | Passed through to scheduling and cache lookup.                                                                   |
| Core RE           | `Execute.execution_policy`                                                                      |    ✅     | The requested priority is persisted and used by the scheduler.                                                   |
| Core RE           | `Execute.results_cache_policy` / cache priorities                                               |           | The request field is carried internally, but cache-priority capabilities are not advertised.                     |
| Core RE           | Inline stdout, stderr, and output files                                                         |           | Buildfarm stores stdout and stderr in the CAS and does not honor `inline_*` hints. Bazel consumes inline payloads when present, but requests `inline_output_files` only for its designated in-memory output and does not request inline stdout or stderr. |
| Core RE           | Additional execution `server_logs`                                                              |           | Buildfarm does not populate the optional `ExecuteResponse.server_logs` map.                                      |
| Core RE           | Output directory formats                                                                        |    ✅     | Produces the default `TREE_ONLY` output-directory representation.                                                |
| Core RE           | Client-requested output filesystem metadata (`Command.output_node_properties`)                  |           | Buildfarm does not advertise or implement output-node-property retrieval. Current Bazel neither requests nor consumes properties such as `mtime` or `unix_mode`; they may matter to other REAPI clients. |
| Core RE           | Long-running Operations: `GetOperation`, `ListOperations`, `DeleteOperation`, `CancelOperation` |    ✅     | The RE API requires only the execution methods; Buildfarm also serves these Operations methods.                  |
| CAS / ByteStream  | `BatchUpdateBlobs`, `BatchReadBlobs`, and paged `GetTree`                                       |    ✅     | Supports the standard batched and tree-transfer RPCs.                                                            |
| CAS / ByteStream  | Resumable uploads (`QueryWriteStatus`)                                                          |    ✅     | Provided through the Google ByteStream service.                                                                  |
| CAS / ByteStream  | Digest algorithms                                                                               |    ✅     | Advertises `SHA256` (the default), `SHA384`, `SHA512`, `SHA1`, `MD5`, and `BLAKE3` for CAS and execution.        |
| CAS / ByteStream  | ByteStream compression                                                                          |    ✅     | Supports uncompressed (`IDENTITY`) and Zstandard (`ZSTD`) `compressed-blobs` transfers; Bazel uses `ZSTD` when remote-cache compression is enabled. |
| CAS / ByteStream  | Deflate or Brotli blob transfer                                                                 |           | `DEFLATE` and `BROTLI` are REAPI-defined compressors but are not advertised or supported by Buildfarm; Bazel does not use them for REAPI compressed-blob transfer. |
| CAS / ByteStream  | Batch-upload compression                                                                        |           | Buildfarm does not advertise `supported_batch_update_compressors`. Current Bazel uploads through ByteStream and uses `supported_compressors` / `ZSTD` instead. |
| CAS / ByteStream  | Absolute symlink targets                                                                        |    ✅     | Configurable; advertised as allowed only when `allowSymlinkTargetAbsolute` is enabled.                           |
| CAS / ByteStream  | FastCDC 2020 content-defined chunking                                                          |    ✅     | Supports `FAST_CDC_2020` with configurable average chunk size and seed (defaults: 512 KiB and `0`); normalization is 2. |
| CAS / ByteStream  | Deprecated `SplitBlob` / `SpliceBlob` chunking RPCs                                             |    ✅     | Deprecated upstream, but required by current Bazel CDC clients when `--experimental_remote_cache_chunking` is enabled. |
| CAS / ByteStream  | Current `GetChunkMapping` / `RegisterChunkMapping` streaming RPCs                               |           | REAPI v2.13+ replacements; not yet exposed by Buildfarm and not currently used by Bazel’s CDC client.             |
| CAS / ByteStream  | RepMaxCDC content-defined chunking                                                              |           | `RepMaxCdcParams` are not advertised.                                                                             |
| Remote Asset      | `Fetch.FetchBlob`                                                                               |    ✅     | Fetches supported URI schemes into the CAS; supports Bazel checksum and HTTP-header qualifiers.                  |
| Remote Asset      | `Fetch.FetchDirectory`                                                                          |           | Returns `UNIMPLEMENTED`. Current Bazel uses `FetchBlob` for its optional remote downloader and does not use this RPC; it may matter to other Remote Asset clients. |
| Remote Asset      | `Push.PushBlob` / `Push.PushDirectory`                                                          |           | The optional Push service is not registered.                                                                     |
| Versioning        | RE API version range                                                                            |    ✅     | `GetCapabilities` advertises non-deprecated RE API versions 2.3 through 2.11.                                    |
| Server extensions | gRPC health checking and reflection                                                             |    ✅     | Registered by the Buildfarm server for service discovery and health checks.                                      |
| Server extensions | gRPC Channelz                                                                                   |    ✅     | Optional server registration via `server.grpcChannelz`.                                                          |

The server’s [`Capabilities.GetCapabilities`][capabilities] response is authoritative for a specific endpoint and instance. Clients should negotiate optional behavior from that response rather than assuming every deployment enables every feature.

## Deploy with Helm

Install the OCI-packaged chart into its own namespace:

```shell
helm install buildfarm oci://ghcr.io/buildfarm/buildfarm \
  --namespace buildfarm \
  --create-namespace \
  --version 0.5.0
```

See the [worker execution environment guide][worker-environment] for worker image composition and the [configuration reference][configuration] for production settings.

## Development

Third-party JVM dependencies are managed with [`rules_jvm_external`][rules-jvm-external] through the `maven.install` declaration in [`MODULE.bazel`](MODULE.bazel). Other external dependencies are declared as Bazel modules or manually managed repository overrides in the same file.

Read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request.

[bazel-remote]: https://bazel.build/remote/caching
[architecture]: https://buildfarm.github.io/buildfarm/docs/architecture/architecture/
[capabilities]: https://github.com/bazelbuild/remote-apis/blob/main/build/bazel/remote/execution/v2/remote_execution.proto#L514
[configuration]: https://buildfarm.github.io/buildfarm/docs/configuration/
[docs]: https://buildfarm.github.io/buildfarm/
[issues]: https://github.com/buildfarm/buildfarm/issues
[observability]: https://buildfarm.github.io/buildfarm/docs/execution/observability/
[quick-start]: https://buildfarm.github.io/buildfarm/docs/quick_start/
[remote-apis]: https://github.com/bazelbuild/remote-apis
[remote-apis-pin]: https://github.com/bazelbuild/remote-apis/tree/6def1c5d27a527c400875c24ae8b1a160145d7e1
[rules-jvm-external]: https://github.com/bazel-contrib/rules_jvm_external
[slack]: https://join.slack.com/t/buildteamworld/shared_invite/zt-3xzjl2hc3-SGhLytWezvmIKD6uwwpI6Q
[worker-environment]: https://buildfarm.github.io/buildfarm/docs/architecture/worker-execution-environment/
