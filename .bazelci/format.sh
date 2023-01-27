#!/bin/bash
# Run from the root of repository.
# This script will format all of the java source files.
# Use the flag --check if you want the script to fail when formatting is not correct.

FORMAT_JAVA=true
REMOVE_NEWLINES_AFTER_START_BRACKET=true
JAVA_FORMATTER_URL=https://github.com/google/google-java-format/releases/download/google-java-format-1.7/google-java-format-1.7-all-deps.jar
LOCAL_FORMATTER="java_formatter.jar"

FORMAT_PROTO=true
CLANG_FORMAT=@llvm_toolchain//:clang-format
if [ -z "$BAZEL" ]; then
  BAZEL=bazel
fi

FORMAT_BUILD=true
BUILDIFIER=//:buildifier

# Print an error such that it will surface in the context of buildkite
print_error () {
    >&2 echo "$1"
    if [ -v BUILDKITE ] ; then
        buildkite-agent annotate "$1" --style 'error' --context 'ctx-error'
    fi
}

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
    fi
    
     # Get all the files to format and format them
    files=$(find src/* -type f -name "*.java")
    
    # Check whether any formatting changes need to be made.
    # This is intended to be done by the CI.
    if [[ "$@" == "--check" ]]
    then
        java -jar $LOCAL_FORMATTER --dry-run --set-exit-if-changed $files
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
    java -jar $LOCAL_FORMATTER -i $files
}

run_proto_formatter () {

    # Check whether any formatting changes need to be made.
    # This is intended to be done by the CI.
    if [[ "$@" == "--check" ]]
    then
        find . -name '*.proto' -exec $BAZEL run $CLANG_FORMAT -- -i --dry-run --Werror {} +
        handle_format_error_check
        return
    fi

    # Fixes formatting issues
    find . -name '*.proto' -exec $BAZEL run $CLANG_FORMAT -- -i {} +
}

run_buildifier () {
    $BAZEL run $BUILDIFIER -- -r > /dev/null 2>&1
}

if [ "${FORMAT_JAVA:-false}" = true ]; then
    run_java_formatter "$@"
fi;

if [ "${FORMAT_PROTO:-false}" = true ]; then
    run_proto_formatter "$@"
fi;

if [ "${FORMAT_BUILD:-false}" = true ]; then
    run_buildifier "$@"
fi;
