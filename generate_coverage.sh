#!/bin/bash
# Run on specifc test targets via: ./generate_coverage.sh <target>
# Script can be run without <target> to get coverage on all tests.

# This script recognizes the following environment variables:

# BUILDFARM_SKIP_COVERAGE_HOST
#     If BUILDFARM_SKIP_COVERAGE_HOST=true, the script will still
#     generage coverage, but it will not convert results to html or start a server.
#     This is useful for ensuring coverage works in CI without attempting to host it.
#
# BUILDFARM_GATE_LCOV_RESULTS
#     If BUILDFARM_GATE_LCOV_RESULTS=true, the script will gate on lcov results.
#     The lcov tool is obtained and used to check code coverage statistics.
#     If the results are below the configured thresholds, the script will fail.

set -e

# This decides how bazel should generate the code coverage files
# Combining coverage files automatically will create a specific coverage report.
COMBINE_REPORT=true

GATING_LINE_PERCENTAGE="40";
GATING_FUNC_PERCENTAGE="40";

# If the user does not pass a given target, this will be used instead.
DEFAULT_TEST_TARGET="//src/test/java/...:all"

# Any test tags filters to apply when generating code coverage.
DEFAULT_TEST_TAG_FILTERS="-redis,-integration"

# How to spawn bazel.  The CI has a bazel available.
DEFAULT_BAZEL_WRAPPER=bazel

# Where test logs will occur.
# Individual code coverage files can be found within this directory.
EXPECTED_TEST_LOGS=bazel-testlogs

# This is where the combined coverage report should be created.
COMBINED_REPORT="bazel-out/_coverage/_coverage_report.dat"

# Settings for obtaining lcov tools.
LCOV_TOOL="lcov"
LCOV_VERSION="1.15"
LCOV_TOOL_URL="https://github.com/linux-test-project/lcov/releases/download/v$LCOV_VERSION/$LCOV_TOOL-$LCOV_VERSION.tar.gz"

# Print an error such that it will surface in the context of buildkite
print_error () {
    >&2 echo "$1"
    if [ -v BUILDKITE ] ; then
        buildkite-agent annotate "$1" --append --style 'error' --context 'ctx-error'
    fi
}

# Download lcov if we don't already have it.
download_lcov() {
    if [ ! -f "$LCOV_TOOL" ] ; then

        #download and extract tool
        wget -O $LCOV_TOOL $LCOV_TOOL_URL
        tar -xf $LCOV_TOOL
        rm $LCOV_TOOL;
        mv $LCOV_TOOL-$LCOV_VERSION/bin/$LCOV_TOOL .;
        rm -rf $LCOV_TOOL-$LCOV_VERSION;
    fi
}

gate_lcov_results() {

    # get lcov results
    download_lcov
    lcov_results=`$LCOV_TOOL --summary $traces 2>&1`

    # extract our percentage numbers
    local line_percentage=$(echo "$lcov_results" | tr '\n' ' ' | awk '{print $8}' | sed 's/.$//')
    local function_percentage=$(echo "$lcov_results" | tr '\n' ' ' | awk '{print $14}' | sed 's/.$//')
    line_percentage=${line_percentage%.*}
    function_percentage=${function_percentage%.*}

    # gate on configured code coverage threshold
    if [ "$line_percentage" -lt "$GATING_LINE_PERCENTAGE" ]; then
        print_error "line coverage is below gating percentage: "
        print_error "$line_percentage < $GATING_LINE_PERCENTAGE"
        exit 1;
    else
        echo "current line coverage: " $line_percentage%
    fi

    if [ "$function_percentage" -lt "$GATING_FUNC_PERCENTAGE" ]; then
        print_error "function coverage is below gating percentage: "
        print_error "$function_percentage < $GATING_FUNC_PERCENTAGE"
        exit 1;
    else
        echo "current function coverage: " $function_percentage%
    fi

    exit 0
}

generate_coverage_files() {
    if [ $COMBINE_REPORT  = true ] ; then
        "${bazel}" coverage $target --combined_report=lcov --test_tag_filters=$DEFAULT_TEST_TAG_FILTERS
    else
        "${bazel}" coverage $target --test_tag_filters=$DEFAULT_TEST_TAG_FILTERS
    fi
}

# store the targets to get test coverage on.
# if no targets are specified we assume all tests.
target=$@
if [ -z "$target" ]
then
      target=$DEFAULT_TEST_TARGET
fi

# decide how to spawn bazel
# we will use the script in the repo as apposed to bazelisk
bazel=$DEFAULT_BAZEL_WRAPPER
coverage_dir=$EXPECTED_TEST_LOGS/coverage

# Perform bazel coverage
generate_coverage_files

# Collect all of the trace files.
# Some trace files may be empty which we will skip over since they fail genhtml
if [ $COMBINE_REPORT  = true ] ; then
    traces=$PWD/$COMBINED_REPORT
else
    traces=$(find $EXPECTED_TEST_LOGS/ ! -size 0 -name coverage.dat | sed "s|^|$PWD/|")
fi

if [ $BUILDFARM_GATE_LCOV_RESULTS  = true ] ; then
    gate_lcov_results
fi

# Establish directory for hosting code coverage
mkdir -p $coverage_dir
rm -fr $coverage_dir/*
ln -s $PWD/src $coverage_dir/src
cd $coverage_dir

# After running coverage, convert the results to HTML and host it locally
if [ "${BUILDFARM_SKIP_COVERAGE_HOST:-false}" = false ]; then
    command -v genhtml >/dev/null 2>&1 || { echo >&2 'genhtml does not exist.  You may need to install lcov.'; exit 1; }
    genhtml -f $traces

    command -v python >/dev/null 2>&1 || { echo >&2 'python could not be found, so the coverage report cannot be locally hosted.'; exit 1; }

    # Get python version
    pythonVersion=`python -c 'import platform; major, minor, patch = platform.python_version_tuple(); print(major);'`

    # Host code coverage based on python version
    if [ $pythonVersion -eq 2 ];then
        python -m SimpleHTTPServer 8080
    else
        python -m http.server 8080
    fi
else
    echo "Skipped coverage hosting."
fi
