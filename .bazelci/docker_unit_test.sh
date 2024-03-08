#!/bin/bash
# This is to run buildfarm's unit tests from within a docker container

# Build a container for unit tests and run them
cp `which bazel` bazel
docker build -t buildfarm .
docker run buildfarm /bin/bash -c "cd buildfarm; ./bazel test --build_tests_only --test_tag_filters=-container,-integration,-redis ..."