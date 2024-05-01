#!/bin/bash
# This is a CI integation test for a typical deployment of buildfarm.
# It ensures that the buildfarm services can startup and function as expected given PR changes.
# All of the needed buildfarm services are initialized (redis, server, worker).
# We ensure that the system can build a set of bazel targets.

# Build a container for buildfarm services
if [ -z "$BAZEL" ]
then
    BAZEL=bazel
fi
BAZEL_BINARY=$(which $BAZEL)
cp $BAZEL_BINARY bazel
docker build -t buildfarm .

# Run redis container
docker run --rm -d --name buildfarm-redis --network host redis:7.2.4 --bind localhost

# Start the servies and do a test build
docker run \
    -v /tmp:/tmp \
    --network host  \
    --env RUN_TEST=$RUN_TEST \
    --env TEST_ARG1=$TEST_ARG1 \
    --env EXECUTION_STAGE_WIDTH=$EXECUTION_STAGE_WIDTH \
    --env BUILDFARM_CONFIG=$BUILDFARM_CONFIG \
    buildfarm buildfarm/.bazelci/test_buildfarm_container.sh
status=$?

docker stop buildfarm-redis

exit $status
