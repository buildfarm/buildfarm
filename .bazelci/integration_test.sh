#!/bin/bash
# Copyright 2021-2026 The Buildfarm Authors. All rights reserved.
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
# Only copy bazel if it doesn't exist (allow pre-existing bazel to be used)
if [ ! -f bazel ] || [ ! -x bazel ]; then
  cp `which bazel` bazel
fi
docker build -t buildfarm .

# Start the servies and do a test build
# Use more memory for tests that need to build execution wrappers
MEMORY_LIMIT=""
CGROUP_OPTIONS=""
if [[ "$BUILDFARM_CONFIG" == *"cgroups-sandbox"* ]]; then
  MEMORY_LIMIT="--memory=4g"
  # For cgroups v2 + linux-sandbox: need privileged mode for mount/namespace operations
  # The client bazel needs to create sandboxes, and the worker needs cgroup access
  CGROUP_OPTIONS="--privileged --cgroupns=host -v /sys/fs/cgroup:/sys/fs/cgroup:rw"
fi

docker run --rm \
    -v /tmp:/tmp \
    --network host  \
    $MEMORY_LIMIT \
    $CGROUP_OPTIONS \
    --env RUN_TEST=$RUN_TEST \
    --env TEST_ARG1=$TEST_ARG1 \
    --env EXECUTION_STAGE_WIDTH=$EXECUTION_STAGE_WIDTH \
    --env BUILDFARM_CONFIG=$BUILDFARM_CONFIG \
    buildfarm buildfarm/.bazelci/test_buildfarm_container.sh
status=$?

docker stop buildfarm-redis

exit $status
