#!/bin/bash
# This is a CI integation test for a typical deployment of buildfarm.
# It ensures that the buildfarm services can startup and function as expected given PR changes.
# All of the needed buildfarm services are initialized (redis, server, worker).
# We ensure that the system can build a set of bazel targets.

# Run redis container
docker run -d --name buildfarm-redis --network host redis:5.0.9 --bind localhost

# Build a container for buildfarm services
cp `which bazel` bazel
docker build -t buildfarm .

# Start the servies and do a test build
docker run \
    -v /tmp:/tmp \
    --network host  \
    --env RUN_TEST=$RUN_TEST \
    --env TEST_ARG1=$TEST_ARG1 \
    --env EXECUTION_STAGE_WIDTH=$EXECUTION_STAGE_WIDTH \
    --env BUILDFARM_CONFIG=$BUILDFARM_CONFIG \
    buildfarm buildfarm/.bazelci/test_buildfarm_container.sh
