#!/bin/bash
# Copyright 2021-2025 The Buildfarm Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This is a CI integation test for a typical deployment of buildfarm.
# It ensures that the buildfarm services can startup and function as expected given PR changes.
# All of the needed buildfarm services are initialized (redis, server, worker).
# We ensure that the system can build a set of bazel targets.

# Run redis container
docker run --rm -d --name buildfarm-redis --network host redis:7.2.4 --bind localhost

# Build a container for buildfarm services
cp `which bazel` bazel
docker build -t buildfarm .

# Start the servies and do a test build
docker run --rm \
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
