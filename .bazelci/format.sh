#!/bin/bash
# Run from the root of repository.
# This script will format all of the java source files.

JAVA_FORMATTER_URL=https://github.com/google/google-java-format/releases/download/google-java-format-1.7/google-java-format-1.7-all-deps.jar
LOCAL_FORMATTER="java_formatter.jar"

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
        >&2 echo "Not all files are correctly formatted."
        >&2 echo "Run ./.bazelci/format.sh to resolve this issue."
        exit 1
    fi
fi

# Fixes formatting issues
java -jar $LOCAL_FORMATTER -r $files 