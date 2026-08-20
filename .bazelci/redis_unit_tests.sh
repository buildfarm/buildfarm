#!/bin/bash
# Copyright 2022-2026 The Buildfarm Authors. All rights reserved.
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

# Typically our redis implementations are mocked.
# However this runs unit tests that interact directly with redis.

# Define Redis details
REDIS_VERSION="7.2.4"
REDIS_DIR="redis-${REDIS_VERSION}"

# Download and compile Redis if not already present
if [ ! -d "$REDIS_DIR" ]; then
    echo "Downloading and compiling Redis ${REDIS_VERSION}..."
    curl -sSL "https://github.com/redis/redis/archive/refs/tags/${REDIS_VERSION}.tar.gz" | tar -xz
    make -C "$REDIS_DIR" -j$(nproc)
fi

# Clean up function to stop services on exit
cleanup() {
    echo "Stopping services..."
    # Gracefully stop Redis, then force kill if needed
    if [ -n "$REDIS_PID" ]; then
        ./"${REDIS_DIR}/src/redis-cli" shutdown 2>/dev/null || kill "$REDIS_PID" 2>/dev/null
    fi
    wait "$REDIS_PID" "$PID2" 2>/dev/null
    echo "All services stopped."
}

# Trap EXIT, SIGINT, and SIGTERM
trap cleanup EXIT INT TERM

# Start Redis 7.2.4 in the background
./"${REDIS_DIR}/src/redis-server" --port 6379 &
REDIS_PID=$!

if [ -z "$BAZEL" ]
then
    BAZEL=bazel
fi

# Run tests that rely on redis
$BAZEL test --build_tests_only --test_tag_filters=redis --test_filter=removeFromDequeueTrueWhenValueExists //src/test/java/build/buildfarm/common/redis:balancedqueue-redis
