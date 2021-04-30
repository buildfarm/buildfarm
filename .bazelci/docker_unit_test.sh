#!/bin/bash
# This is to run buildfarm's unit tests from within a docker container

# Build a container for unit tests and run them
docker build -t buildfarm .
docker run buildfarm /bin/bash -c "cd buildfarm; ./bazelw test --build_tests_only ..."