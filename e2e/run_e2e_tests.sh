#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
bazelisk run //container:buildfarm-server.load
bazelisk run //container:buildfarm-worker.load

setup() {
    (cd "$script_dir/.setup_buildfarm" && docker compose up --detach)
}
cleanup() {
    (cd "$script_dir/.setup_buildfarm" && docker compose down --volumes) || true
}

for dir in "$script_dir"/*/; do
    (
        echo "Running tests in $dir"
        setup
        trap cleanup EXIT

        cd "$dir"
        bazelisk clean
        bazelisk test --test_output=errors //...
    )
done
