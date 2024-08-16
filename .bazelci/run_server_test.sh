#!/bin/bash

# Start redis container
docker run -d --rm --name buildfarm-redis --network host redis:7.2.4 --bind localhost

# Build worker and server targets
bazel build //src/main/java/build/buildfarm:buildfarm-shard-worker
bazel build //src/main/java/build/buildfarm:buildfarm-server

# Start a single worker
bazel run //src/main/java/build/buildfarm:buildfarm-shard-worker $(pwd)/examples/config.minimal.yml > worker.log 2>&1 &
worker_pid=$!
echo "Started buildfarm-shard-worker..."

# Start a single server
bazel run //src/main/java/build/buildfarm:buildfarm-server $(pwd)/examples/config.minimal.yml > server.log 2>&1 &
server_pid=$!
echo "Started buildfarm-server..."

echo "Wait for startup to finish..."
sleep 5
echo "Printing server initialization logs..."
cat server.log
echo "Printing worker initialization logs..."
cat worker.log

# Build bazel targets with buildfarm
echo "Running server integration tests..."
bazel test --nocache_test_results --test_tag_filters=integration src/test/java/build/buildfarm/server:all
status=$?

worker_pid=$(ps --ppid $worker_pid -o pid=)
server_pid=$(ps --ppid $server_pid -o pid=)

kill -INT $worker_pid $server_pid

sleep 5

docker stop buildfarm-redis

exit $status
