#!/bin/bash
# Run from the root of repository.
# This script will perform static analysis on all of the java source files.

PMD_TOOL_URL=https://github.com/pmd/pmd/releases/download/pmd_releases%2F6.32.0/pmd-bin-6.32.0.zip
LOCAL_DOWNLOAD_NAME="pmd.zip"
TOOL_FOLDER="pmd"
RULESET=".bazelci/static_analysis_checks.xml"
REPORT_FILE="/tmp/static_analysis.txt"

# Print an error such that it will surface in the context of buildkite
print_error () {
    >&2 echo "$1"
    if [ -v BUILDKITE ] ; then
        buildkite-agent annotate "$1" --append --style 'error' --context 'ctx-error'
    fi
}

# Download tool if we don't already have it.
download_tool () {
    if [ ! -d "$TOOL_FOLDER" ] ; then
        
        #download and extract tool
        wget -O $LOCAL_DOWNLOAD_NAME $PMD_TOOL_URL
        unzip $LOCAL_DOWNLOAD_NAME
        
        #delete the zip and give tool common name.
        rm $LOCAL_DOWNLOAD_NAME
        mv pmd-* $TOOL_FOLDER;
    fi
}

# The tool should return non-zero if there are violations
run_tool () {
    pmd/bin/run.sh pmd -R $RULESET -format text -shortnames -reportfile $REPORT_FILE -dir src
}

analyze_results () {
    if [ $? -eq 0 ]
    then
       echo "Code has passed static analysis.  "
       exit 0
    else
        # Show the errors both in a buildkite message and the terminal.
        print_error "Code has not passed static analysis.  "
        while read line; do
            print_error "$line  "
        done <$REPORT_FILE
        exit 1
    fi
}

# Perform static analysis
download_tool
run_tool
analyze_results