#!/bin/bash
# Run from the root of repository.
# This script will perform static analysis on all of the java source files.

TOOL_URL=https://github.com/checkstyle/checkstyle/releases/download/checkstyle-10.17.0/checkstyle-10.17.0-all.jar
LOCAL_DOWNLOAD_NAME="checkstyle.jar"
TOOL_FOLDER="checkstyle"

# Settings for static analysis checks
RUN_STATIC_ANALYSIS_CHECKS=true
STATIC_ANALYSIS_RULESET=".bazelci/checkstyle_checks.xml"
STATIC_ANALYSIS_REPORT_FILE="/tmp/checkstyle.txt"


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

        #download tool
        wget -O $LOCAL_DOWNLOAD_NAME $TOOL_URL

        #delete the zip and give tool common name.
        mkdir $TOOL_FOLDER;
        mv checkstyle.jar $TOOL_FOLDER;
    fi
}

# The tool should return non-zero if there are violations
run_static_analysis_checks () {
    java -jar checkstyle/checkstyle.jar -c "$STATIC_ANALYSIS_RULESET" -o "$STATIC_ANALYSIS_REPORT_FILE" src
}

analyze_static_analysis_results () {
    if [ $? -eq 0 ]
    then
       echo "Code has passed static analysis.  "
    else
        # Show the errors both in a buildkite message and the terminal.
        print_error "Code has not passed static analysis.  "
        while read line; do
            print_error "$line  "
        done <$STATIC_ANALYSIS_REPORT_FILE
        exit 1
    fi
}

main () {

    # Get tools needed to run static analysis
    download_tool

    # Possibly run static analysis checks
    if [ "${RUN_STATIC_ANALYSIS_CHECKS:-false}" = true ]; then
        run_static_analysis_checks
        analyze_static_analysis_results
    fi;

}

main
