# Content-defined chunking fixture

The dedicated CI runner copies this workspace into a fresh artifact directory,
generates an 8 MiB SHAKE-256 byte stream, and remotely runs one shell action that
prepends a fixed marker. The input and output therefore have distinct, known
SHA-256 digests, both above Buildfarm's default 2 MiB FastCDC threshold. The
analyzer also checks the actual advertised threshold.

Generation is deterministic for a given seed. The runner records a new seed per
run to keep the cold upload/action unique without deleting any shared cache.
Use the seed in `manifest.json` to reproduce the input with `fixture.py prepare`.
The runner uses independent cold/cached output bases and disables disk caching,
local fallback, and user/workspace rc files. Both builds download and verify the
artifact. The analyzer checks the fixture digests, action identity, chunk
metadata, and chunk transfers; the cached build must not execute or upload.

CI entry point: `.bazelci/cache_test.sh`, configured by the Content Defined
Chunking task in `.bazelci/presubmit.yml`. To run against an existing cluster at
`localhost:8980`, set `CDC_ARTIFACTS` to a fresh directory and `CDC_BAZEL` to an
absolute Bazel/Bazelisk executable, then run
`.bazelci/run_content_defined_chunking_test.sh`. Logs, manifest, version, and
isolated build state are retained there for diagnosis. The fixture action uses
the worker's `/bin/cat` and shell, not a compiler toolchain.
