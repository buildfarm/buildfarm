# Content-defined chunking fixture

The dedicated CI runner copies this workspace into a fresh artifact directory,
generates an 8 MiB SHAKE-256 byte stream, and runs one shell action that
prepends a fixed marker. The input and output therefore have distinct, known
SHA-256 digests, both above Buildfarm's default 2 MiB FastCDC threshold. The
analyzer also checks the actual advertised threshold.

Generation is deterministic for a given seed. The runner records a new seed per
run to keep the cold upload/action unique without deleting any shared cache.
Use the seed in `manifest.json` to reproduce the input with `fixture.py prepare`.
The runner uses three phases with independent output bases (including local chunk
state), no disk cache or user/workspace rc files, and no remote execution fallback:

1. **Upload:** run the action locally with a remote cache, reject cached results,
   and synchronously upload local results (`--remote_cache_async=false`). The
   analyzer requires successful output `SpliceBlob` and chunk writes, with no
   whole-output upload fallback or remote execution.
2. **Cold:** force remote execution, skipping action-cache lookup, then download
   and verify the output. Whole-input ByteStream writes are expected here.
3. **Cached:** require a remote action-cache hit for the cold action and a
   successful output `SplitBlob`, reading every distinct chunk without a
   whole-output read fallback. No execution or uploads are allowed.

All three phases verify the artifact. Upload runs first with a fresh fixture
digest so existing remote output/chunks cannot bypass the upload path. In Bazel
9.3.0, [`RemoteExecutionCache.uploadFile`](https://github.com/bazelbuild/bazel/blob/9.3.0/src/main/java/com/google/devtools/build/lib/remote/RemoteExecutionCache.java)
uploads action inputs directly through the remote client, bypassing CDC. Local
outputs instead follow `RemoteExecutionService.uploadOutputs` → `UploadManifest`
→ [`CombinedCache.uploadFile`](https://github.com/bazelbuild/bazel/blob/9.3.0/src/main/java/com/google/devtools/build/lib/remote/CombinedCache.java),
which chunks files exceeding the advertised maximum chunk size. Synchronous mode
awaits blob uploads and the action-cache update before the build/log closes.

Decode `upload.grpc.log`, `cold.grpc.log`, and `cached.grpc.log`, then validate in
that order:

```sh
python3 .bazelci/ingest_grpc_logs.py --cdc-manifest manifest.json \
  upload.grpc.json cold.grpc.json cached.grpc.json
```

The baseline two-log invocation without `--cdc-manifest` is unchanged.

CI entry point: `.bazelci/cache_test.sh`, configured by the Content Defined
Chunking task in `.bazelci/presubmit.yml`. To run against an existing cluster at
`localhost:8980`, set `CDC_ARTIFACTS` to a fresh directory and `CDC_BAZEL` to an
absolute Bazel/Bazelisk executable, then run
`.bazelci/run_content_defined_chunking_test.sh`. Logs, manifest, version, and
isolated build state are retained there for diagnosis. The fixture action uses
`/bin/cat` and a shell on the local host and worker, not a compiler toolchain.
