#!/usr/bin/env bash
# Run from the root of repository.
# This script will format all of the java source files.
# Use the flag --check if you want the script to fail when formatting is not correct.

FORMAT_JAVA=true
REMOVE_NEWLINES_AFTER_START_BRACKET=true

# Print an error such that it will surface in the context of buildkite
print_error () {
    >&2 echo "$1"
    if [ -n "$BUILDKITE" ] ; then
        buildkite-agent annotate "$1" --style 'error' --context 'ctx-error'
    fi
}

# Get the operating system
os=$(uname | tr '[:upper:]' '[:lower:]')

# Get the machine architecture
arch=$(uname -m | tr '_' '-')

# Set the correct Google Java Formatter URL based on the operating system and architecture
JAVA_FORMATTER_URL="https://github.com/google/google-java-format/releases/download/v1.23.0/google-java-format_${os}-${arch}"
LOCAL_FORMATTER="./google-java-format"

if [ -z "$BAZEL" ]; then
  BAZEL=bazel
fi

FORMAT_BUILD=true
BUILDIFIER=//:buildifier


handle_format_error_check () {
    if [ $? -eq 0 ]
    then
        echo "Files are correctly formatted."
    else
        print_error 'Run ./.bazelci/format.sh to resolve formatting issues.'
        exit 1
    fi
}

run_java_formatter () {

     # Download the formatter if we don't already have it.
    if [ ! -f "$LOCAL_FORMATTER" ] ; then
        wget -O $LOCAL_FORMATTER $JAVA_FORMATTER_URL
        chmod +x $LOCAL_FORMATTER
    fi

     # Get all the files to format and format them
    files=$(find src/* persistentworkers/* -type f -name "*.java")

    # Check whether any formatting changes need to be made.
    # This is intended to be done by the CI.
    if [[ "$@" == "--check" ]]
    then
        $LOCAL_FORMATTER --dry-run --set-exit-if-changed $files
        handle_format_error_check
        return
    fi

    # The formatter is lax on certain whitespace decisions.
    # Therefore, we perform these adjustements before running the formatter.
    # This will make the formatting more consistent overall.
    if [ "${REMOVE_NEWLINES_AFTER_START_BRACKET:-false}" = true ]; then
        for file in $files
        do
    	# Remove whitespace lines after starting bracket '{'
    	# Ignore any issues if this does not succeed.
    	# The CI doesn't gate on this adjustment.
            awk -i inplace -v n=-2 'NR==n+1 && !NF{next} /\{/ {n=NR}1' $file > /dev/null 2>&1
        done
    fi;

    # Fixes formatting issues
    $LOCAL_FORMATTER -i $files
}

run_buildifier () {
    $BAZEL run $BUILDIFIER -- -r > /dev/null 2>&1
}

if [ "${FORMAT_JAVA:-false}" = true ]; then
    run_java_formatter "$@"
fi;

if [ "${FORMAT_BUILD:-false}" = true ]; then
    run_buildifier "$@"
fi;
