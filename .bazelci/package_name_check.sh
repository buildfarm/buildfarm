#!/bin/bash
# Run from the root of repository.
# This script will analyze all source files to ensure their package names are specified correctly.
# Bazel coverage does not work properly if the package name does not match the file layout.

# Print an error such that it will surface in the context of buildkite
print_error () {
    >&2 echo "$1"
    if [ -v BUILDKITE ] ; then
        buildkite-agent annotate "$1" --style 'error' --context 'ctx-error'
    fi
}

extract_package_name () {
        local file=$1
        local package_name=`sed -n '/^package /p' $file | head -n 1`
        package_name=${package_name:8}
        package_name=${package_name::-1}
        echo $package_name
}

# Get all of the files to analyze
files=$(find src/* -type f -name "*.java")

for file in $files
do
        current_package_name=$(extract_package_name $file)
	if [ -z "$current_package_name" ]
	then
	      echo "\$var is empty" $current_package_name
	else
	      echo "\$var is NOT empty" $current_package_name
	fi
done