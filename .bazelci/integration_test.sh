#!/bin/bash
# This is a CI integation test for a typical "shard deployment" of buildfarm.
# It ensures that the buildfarm services can startup and function as expected given PR changes.
# All of the needed buildfarm services are initialized (redis, server, worker).
# We ensure that the system can build a set of bazel targets.

# Run redis container
docker run -d --name buildfarm-redis -p 6379:6379 redis:5.0.9

# Build a container for buildfarm services
docker build -t buildfarm .

# Start the servies and do a test build
docker run --network host buildfarm buildfarm/.bazelci/test_buildfarm_container.sh