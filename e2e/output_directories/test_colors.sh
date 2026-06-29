#!/usr/bin/env bash
set -euo pipefail

expected_files=("blue.txt" "red.txt")
actual_files=($(ls -1 "colors"))

if [[ "${expected_files[*]}" != "${actual_files[*]}" ]]; then
	echo "Expected files: ${expected_files[*]}" >&2
	echo "Actual files: ${actual_files[*]}" >&2
	exit 1
fi

