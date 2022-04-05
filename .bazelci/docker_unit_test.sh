#!/bin/bash
# This is to run buildfarm's unit tests from within a docker container

# Build a container for unit tests and run them
cp `which bazel` bazel
docker build -t buildfarm .
docker run /bin/bash -c "ls; ls buildfarm; cd buildfarm; ./bazel test --build_tests_only ..."