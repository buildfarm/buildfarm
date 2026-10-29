#!/usr/bin/env bash
# Copyright 2026 The Buildfarm Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BAZEL_BIN=${CDC_BAZEL:-"$ROOT/bazel"}
ARTIFACTS=${CDC_ARTIFACTS:?Set CDC_ARTIFACTS to a fresh directory for logs and fixture state}
mkdir -p "$ARTIFACTS"
ARTIFACTS=$(cd "$ARTIFACTS" && pwd)
# Refuse stale state rather than deleting anything, including shared caches.
mkdir "$ARTIFACTS/workspace"
cp "$ROOT/src/test/cdc/"{MODULE.bazel,BUILD.bazel,fixture.bzl,.bazelversion} "$ARTIFACTS/workspace/"
SEED=$(python3 -c 'import uuid; print(uuid.uuid4().hex)')
python3 "$ROOT/src/test/cdc/fixture.py" prepare --seed "$SEED" \
    --workspace "$ARTIFACTS/workspace" --manifest "$ARTIFACTS/manifest.json"

cd "$ARTIFACTS/workspace"
# Bazelisk is pinned by the copied .bazelversion. Record the actual executable
# version and fail before the build if it lacks the experimental flag.
"$BAZEL_BIN" --ignore_all_rc_files --batch version > "$ARTIFACTS/bazel-version.txt"
cat "$ARTIFACTS/bazel-version.txt"
"$BAZEL_BIN" --ignore_all_rc_files --batch help build > "$ARTIFACTS/bazel-build-help.txt"
grep -q -- 'experimental_remote_cache_chunking ' "$ARTIFACTS/bazel-build-help.txt"

COMMON=(--remote_executor=grpc://localhost:8980
    --experimental_remote_cache_chunking
    --experimental_remote_cache_chunking_function=fast_cdc_2020
    --remote_download_outputs=all --remote_verify_downloads
    --disk_cache= --noremote_local_fallback --noremote_upload_local_results
    --spawn_strategy=remote --strategy=CdcFixture=remote
    --lockfile_mode=off --symlink_prefix=bazel-)

for PHASE in cold cached; do
    # Separate output bases isolate action outputs AND the local CDC chunk cache.
    # Batch mode leaves no additional Bazel servers to clean up.
    FLAGS=(--remote_accept_cached)
    if [[ "$PHASE" == cold ]]; then
        FLAGS=(--noremote_accept_cached)
    fi
    "$BAZEL_BIN" --ignore_all_rc_files --batch --output_base="$ARTIFACTS/$PHASE-output-base" \
        build //:artifact "${COMMON[@]}" "${FLAGS[@]}" \
        --remote_grpc_log="$ARTIFACTS/$PHASE.grpc.log"
    python3 "$ROOT/src/test/cdc/fixture.py" verify --manifest "$ARTIFACTS/manifest.json" \
        --output "$ARTIFACTS/workspace/bazel-bin/artifact.bin"
done
echo "CDC phase logs and manifest retained in $ARTIFACTS"
