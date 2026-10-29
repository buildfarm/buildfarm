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

set -e
set -o pipefail

# Clean up function to stop services on exit
buildfarm_cleanup() {
    set +e
    echo "Stopping buildfarm services..."
    if [ -n "$SERVER_PID" ]; then
        kill "$SERVER_PID" 2>/dev/null
        wait "$SERVER_PID" 2>/dev/null
    else
        echo "No server pid..."
    fi
    if [ -n "$WORKER_PID" ]; then
        kill "$WORKER_PID" 2>/dev/null
        wait "$WORKER_PID" 2>/dev/null
    else
        echo "No server pid..."
    fi
}

#Various targets to be tested
BUILDFARM_SERVER_TARGET="//src/main/java/build/buildfarm:buildfarm-server"
BUILDFARM_WORKER_TARGET="//src/main/java/build/buildfarm:buildfarm-shard-worker"

#The configs used by the targets
BUILDFARM_SERVER_CONFIG=$BUILDFARM_CONFIG
BUILDFARM_WORKER_CONFIG=$BUILDFARM_CONFIG

GRPC_LOGS1="/tmp/parsed-grpc.log"
GRPC_LOGS2="/tmp/parsed-grpc2.log"
RAW_GRPC_LOGS1=${RAW_GRPC_LOGS1:-src/test/many/grpc.log}
RAW_GRPC_LOGS2=${RAW_GRPC_LOGS2:-src/test/many/grpc2.log}
if [ "${CACHE_EXPECTATIONS:-baseline}" = cdc ]; then
    RAW_GRPC_LOGS_UPLOAD="$CDC_ARTIFACTS/upload.grpc.log"
    GRPC_LOGS_UPLOAD="$CDC_ARTIFACTS/upload.grpc.json"
    RAW_GRPC_LOGS1="$CDC_ARTIFACTS/cold.grpc.log"
    RAW_GRPC_LOGS2="$CDC_ARTIFACTS/cached.grpc.log"
    GRPC_LOGS1="$CDC_ARTIFACTS/cold.grpc.json"
    GRPC_LOGS2="$CDC_ARTIFACTS/cached.grpc.json"
fi

ensure_server_is_up(){
    # We cannot do a test build until the server is properly started.
    # In order to determine when the server is started, we can watch the server's log for the proper startup message.
    # We also capture the PID to monitor any crashes.
    # In the future, we will want a better way to confirm the buildfarm cluster is ready to take on work.
    FILE_TO_CHECK="server.log"
    LINE_TO_CONTAIN="initialized"
    SLEEP_TIME=10
    COUNT=0
    MAX_COUNT=5

    # Wait for the server to start
    while [[ ! $(grep $LINE_TO_CONTAIN $FILE_TO_CHECK) ]]
    do
        echo "Waiting for Server to start..."
        sleep ${SLEEP_TIME}
        COUNT=$(($COUNT + 1))

       # Give up waiting for the server to start.
       if [ $COUNT -eq $MAX_COUNT ]; then
          echo "Server did not start."
          break
       fi

       # Give up because the server crashed.
       if [ ! -n "$(ps -p $SERVER_PID -o pid=)" ]; then
          echo "Server crashed."
          break
       fi
    done
}

ensure_worker_is_up(){
    # We cannot do a test build until the worker is properly started and writable.
    # In order to determine when the worker is started, we can watch the worker's log for the proper startup message.
    # We also capture the PID to monitor any crashes.
    # In the future, we will want a better way to confirm the buildfarm cluster is ready to take on work.
    FILE_TO_CHECK="worker.log"
    LINE_TO_CONTAIN=" initialized$"
    SLEEP_TIME=10
    COUNT=0
    MAX_COUNT=5

    # Wait for the server to start
    while [[ ! $(grep $LINE_TO_CONTAIN $FILE_TO_CHECK) ]]
    do
        echo "Waiting for Worker to start..."
        sleep ${SLEEP_TIME}
        COUNT=$(($COUNT + 1))

       # Give up waiting for the worker to start.
       if [ $COUNT -eq $MAX_COUNT ]; then
          echo "Worker did not start."
          break
       fi

       # Give up because the worker crashed.
       if [ ! -n "$(ps -p $WORKER_PID -o pid=)" ]; then
          echo "Worker crashed."
          break
       fi
    done
}

check_for_crashes(){
    while :
    do
        if [ ! -n "$(ps -p $SERVER_PID -o pid=)" ]; then
	    echo "Server crashed."
            cat server.log
            exit -1
	fi
        if [ ! -n "$(ps -p $WORKER_PID -o pid=)" ]; then
	    echo "Worker crashed."
            cat worker.log
            exit -1
	fi
    done
}

start_server_and_worker(){
  echo "Testing with Shard Instances."

  # Build first to create more predictable run time.
  ./bazel build --config=strict $BUILDFARM_SERVER_TARGET $BUILDFARM_WORKER_TARGET

  # Start the server.
  ./bazel run --config=strict $BUILDFARM_SERVER_TARGET -- $BUILDFARM_SERVER_CONFIG > server.log 2>&1 &
  SERVER_PID=$!

  ensure_server_is_up

  # Start the worker.
  ./bazel run --config=strict $BUILDFARM_WORKER_TARGET -- $BUILDFARM_WORKER_CONFIG > worker.log 2>&1 &
  WORKER_PID=$!

  ensure_worker_is_up
}

init_grpc_parser(){
    if [ "${CACHE_TEST:-false}" = true ]; then
        echo "Fetch tools_remote from git"

        git clone https://github.com/buildfarm/tools_remote.git
        pushd tools_remote
        git reset --hard $SHA1_TOOLS_REMOTE
        ../bazel build //:remote_client
        popd
    fi
}
parse_grpc_logs(){
    if [ "${CACHE_TEST:-false}" = true ]; then
        echo "Parse grpc log"
        ./tools_remote/bazel-bin/remote_client --grpc_log "$RAW_GRPC_LOGS1" printlog --format_json > "$GRPC_LOGS1"
        ./tools_remote/bazel-bin/remote_client --grpc_log "$RAW_GRPC_LOGS2" printlog --format_json > "$GRPC_LOGS2"
        if [ "${CACHE_EXPECTATIONS:-baseline}" = cdc ]; then
            ./tools_remote/bazel-bin/remote_client --grpc_log "$RAW_GRPC_LOGS_UPLOAD" printlog --format_json > "$GRPC_LOGS_UPLOAD"
            calculate_cache_statistics "$GRPC_LOGS_UPLOAD" "$GRPC_LOGS1" "$GRPC_LOGS2"
        else
            calculate_cache_statistics "$GRPC_LOGS1" "$GRPC_LOGS2"
        fi
    fi
}

# Trap EXIT, SIGINT, and SIGTERM
trap buildfarm_cleanup EXIT INT TERM

calculate_cache_statistics(){
    echo "Calculating cache statistics..."
    if [ "${CACHE_EXPECTATIONS:-baseline}" = cdc ]; then
        python3 ./.bazelci/ingest_grpc_logs.py --cdc-manifest "$CDC_ARTIFACTS/manifest.json" "$1" "$2" "$3"
    else
        python3 ./.bazelci/ingest_grpc_logs.py "$1" "$2"
    fi
}
start_server_and_worker

# Show startup logs
echo "Server log:"
cat server.log
echo "Worker log:"
cat server.log

check_for_crashes &

init_grpc_parser

#Run a test against the cluster
$RUN_TEST $TEST_ARG1 $TEST_ARG2

parse_grpc_logs
