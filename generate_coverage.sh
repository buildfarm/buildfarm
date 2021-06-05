#!/bin/sh
# Run on specifc test targets via: ./generate_coverage.sh <target>
# Script can be run without <target> to get coverage on all tests.

# This script recognizes the following environment variables:

# BUILDFARM_SKIP_COVERAGE_HOST
#     If BUILDFARM_SKIP_COVERAGE_HOST=true, the script will still
#     generage coverage, but it will not convert results to html and start a server.
#     This is useful for ensuring coverage works in CI without attempting to host it.
set -e

DEFAULT_TEST_TARGET="//src/test/...:all"
DEFAULT_TEST_TAG_FILTERS="-redis"
DEFAULT_BAZEL_WRAPPER=bazel
EXPECTED_TEST_LOGS=bazel-testlogs

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
COVERAGE=$EXPECTED_TEST_LOGS/coverage

# Perform bazel coverage
"${bazel}" coverage $target --test_tag_filters=$DEFAULT_TEST_TAG_FILTERS

# Collect all of the trace files.
# Some trace files may be empty which we will skip over since they fail genhtml
mkdir -p $COVERAGE
traces=$(find $EXPECTED_TEST_LOGS/ ! -size 0 -name coverage.dat | sed "s|^|$PWD/|")
rm -fr $COVERAGE/*
ln -s $PWD/src $COVERAGE/src
cd $COVERAGE

# After running coverage, convert the results to HTML and host it locally
if [ "${BUILDFARM_SKIP_COVERAGE_HOST:-false}" = false ]; then
    command -v genhtml >/dev/null 2>&1 || { echo >&2 'genhtml does not exist.  You may need to install lcov.'; exit 1; }
    genhtml -f $traces

    command -v python >/dev/null 2>&1 || { echo >&2 'python could not be found, so the coverage report cannot be locally hosted.'; exit 1; }
    python -m SimpleHTTPServer 8080
else
    echo "Skipped coverage hosting."
fi
