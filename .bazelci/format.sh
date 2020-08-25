#!/bin/bash
# Run from the root of repository.
# This script will format all of the java source files.

JAVA_FORMATTER_URL=https://github.com/google/google-java-format/releases/download/google-java-format-1.7/google-java-format-1.7-all-deps.jar
LOCAL_FORMATTER="java_formatter.jar"

# Print an error such that it will surface in the context of buildkite
print_error () {
    >&2 echo "$1"
if [ -v BUILDKITE ] ; then
        buildkite-agent annotate "$1" --style 'error' --context 'ctx-error'
    fi
}

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
    if [ $? -eq 0 ]
    then
       echo "Files are correctly formatted."
       exit 0
    else
        print_error 'Run ./.bazelci/format.sh to resolve formatting issues.'
        exit 1
    fi
fi

# Fixes formatting issues
java -jar $LOCAL_FORMATTER -i $files 