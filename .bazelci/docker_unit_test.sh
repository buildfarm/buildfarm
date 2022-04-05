#!/bin/bash
# This is to run buildfarm's unit tests from within a docker container

# Build a container for unit tests and run them


which bazel
readlink -f `which bazel`
file bazel
cp `which bazel` bazel_copy
docker build -t buildfarm .
docker run -v bazel_copy:/usr/bin/bazel buildfarm /bin/bash -c "ls; ls buildfarm; ls -al /usr/bin/bazel; file /usr/bin/bazel; cd buildfarm; /usr/bin/bazel test --build_tests_only ..."