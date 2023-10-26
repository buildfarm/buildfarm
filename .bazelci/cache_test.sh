#!/bin/bash
# This is a CI integation test for a typical eployment of buildfarm.
# It ensures that the buildfarm services can startup and function as expected given PR changes.
# All of the needed buildfarm services are initialized (redis, server, worker).
# We ensure that the system can build a set of bazel targets.

# Run redis container
docker run -d --name buildfarm-redis --network host redis:7.2.4 --bind localhost

# Build a container for buildfarm services
cp `which bazel` bazel
docker build -t buildfarm .

#Start the servies and do a test build
docker run \
    -v /tmp:/tmp \
    --network host  \
    --env CACHE_TEST=$CACHE_TEST \
    --env BUILDFARM_CONFIG=$BUILDFARM_CONFIG \
    --env RUN_TEST=$RUN_TEST \
    --env TEST_ARG1=$TEST_ARG1 \
    --env TEST_ARG2=$TEST_ARG2 \
    --env SHA1_TOOLS_REMOTE=$SHA1_TOOLS_REMOTE \
    buildfarm buildfarm/.bazelci/test_buildfarm_container.sh
