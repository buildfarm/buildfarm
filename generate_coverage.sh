#!/bin/sh
# Run on test targets
# ex: ./generate_coverage.sh //src/test/...:all

set -e

# store the targets to get test coverage on.
# if no targets are specified we assume all tests.
target=$@
if [ -z "$target" ]
then
      target="//src/test/...:all"
fi

bazel=bazelisk
COVERAGE=bazel-testlogs/coverage

"${bazel}" coverage $target --test_tag_filters="-redis"
mkdir -p $COVERAGE
traces=$(find bazel-testlogs/ -name coverage.dat | sed "s|^|$PWD/|")
rm -fr $COVERAGE/*
ln -s $PWD/src $COVERAGE/src
cd $COVERAGE
genhtml -f $traces
python -m http.server
