#!/bin/sh

set -e
bazel coverage $@
COVERAGE=bazel-testlogs/coverage
mkdir -p $COVERAGE
traces=$(find bazel-testlogs/ -name coverage.dat | sed "s|^|$PWD/|")
rm -fr $COVERAGE/*
ln -s $PWD/src/main/java/build $COVERAGE/build
cd $COVERAGE
genhtml -f $traces
python -m SimpleHTTPServer
