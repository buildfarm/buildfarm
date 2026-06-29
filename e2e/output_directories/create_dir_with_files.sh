#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 2 ]]; then
	echo "Usage: $0 {colors|fruits} <output_dir>" >&2
	exit 1
fi

case "$1" in
	colors)
		files=(red blue)
		;;
	fruits)
		files=(apple banana mango)
		;;
	*)
		echo "Unknown input '$1'. Expected 'colors' or 'fruits'." >&2
		exit 1
		;;
esac

output_dir="$2"
mkdir -p "$output_dir"
for file in "${files[@]}"; do
	touch "$output_dir/$file.txt"
done
