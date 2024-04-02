#!/bin/bash

# Start redis container
docker run -d --rm --name buildfarm-redis --network host redis:7.2.4 --bind localhost

if [ -z "$BAZEL" ]
then
    BAZEL=bazel
fi
# Build worker and server targets
$BAZEL build //src/main/java/build/buildfarm:buildfarm-shard-worker
$BAZEL build //src/main/java/build/buildfarm:buildfarm-server

# Start a single worker
$BAZEL run //src/main/java/build/buildfarm:buildfarm-shard-worker $(pwd)/examples/config.minimal.yml > worker.log 2>&1 &
server_pid=$!
echo "Started buildfarm-shard-worker..."

# Start a single server
$BAZEL run //src/main/java/build/buildfarm:buildfarm-server $(pwd)/examples/config.minimal.yml > server.log 2>&1 &
worker_pid=$!
echo "Started buildfarm-server..."

echo "Wait for startup to finish..."
sleep 30
echo "Printing server initialization logs..."
cat server.log
echo "Printing worker initialization logs..."
cat worker.log

# Build bazel targets with buildfarm
echo "Running server integration tests..."
$BAZEL test --test_tag_filters=integration src/test/java/build/buildfarm/server:all
status=$?

kill $worker_pid $server_pid

wait $server_pid
wait $worker_pid

docker stop buildfarm-redis

exit $status
