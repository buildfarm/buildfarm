#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"

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
        bazel clean
        bazel test --test_output=errors //...
    )
done
