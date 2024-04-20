#!/bin/bash
# Run from the root of repository.
# This script will perform static analysis on all of the java source files.

PMD_TOOL_URL=https://github.com/pmd/pmd/releases/download/pmd_releases%2F6.55.0/pmd-bin-6.55.0.zip
LOCAL_DOWNLOAD_NAME="pmd.zip"
TOOL_FOLDER="pmd"

# Settings for static analysis checks
RUN_STATIC_ANALYSIS_CHECKS=true
STATIC_ANALYSIS_RULESET=".bazelci/static_analysis_checks.xml"
STATIC_ANALYSIS_REPORT_FILE="$(mktemp -t static_analysis.txt)"

# Settings for code duplication detection
RUN_CODE_DUPLICATION_CHECK=true
CODE_DUPLICATION_MIN_TOKENS=600
CODE_DUPLICATION_FORMAT="csv"
CODE_DUPLICATION_REPORT_FILE="/tmp/code_duplication.csv"


# Print an error such that it will surface in the context of buildkite
print_error () {
    >&2 echo "$1"
    if [ -n "$BUILDKITE" ] ; then
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
run_static_analysis_checks () {
    pmd/bin/run.sh pmd -R $STATIC_ANALYSIS_RULESET --format text --relativize-paths-with src --report-file $STATIC_ANALYSIS_REPORT_FILE --dir src
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

run_code_duplication_check () {
    pmd/bin/run.sh cpd --format $CODE_DUPLICATION_FORMAT --minimum-tokens $CODE_DUPLICATION_MIN_TOKENS --dir src/main src/test/java > $CODE_DUPLICATION_REPORT_FILE
}

analyze_code_duplication_results () {
    if [ $? -eq 0 ]
    then
       echo "Code has passed duplication detection.  "
    else
        # Show the errors both in a buildkite message and the terminal.
        print_error "Code has not passed duplication detection.  "
        while read line; do
            print_error "$line  "
        done <$CODE_DUPLICATION_REPORT_FILE
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

    # Possibly run code duplication check
    if [ "${RUN_CODE_DUPLICATION_CHECK:-false}" = true ]; then
        run_code_duplication_check
        analyze_code_duplication_results
    fi;
    
}

main