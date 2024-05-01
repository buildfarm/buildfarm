#!/bin/bash
set -e
set -o pipefail

# This script is to be called within a built container reflecting the changes of a PR
# We start the server and the worker, and test that they can complete builds for a bazel client.
cd buildfarm;

#Various targets to be tested
BUILDFARM_SERVER_TARGET="//src/main/java/build/buildfarm:buildfarm-server"
BUILDFARM_WORKER_TARGET="//src/main/java/build/buildfarm:buildfarm-shard-worker"

#The configs used by the targets
BUILDFARM_SERVER_CONFIG=$BUILDFARM_CONFIG
BUILDFARM_WORKER_CONFIG=$BUILDFARM_CONFIG

GRPC_LOGS1="/tmp/parsed-grpc.log"
GRPC_LOGS2="/tmp/parsed-grpc2.log"

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
    while [[ ! $(grep $LINE_TO_CONTAIN $FILE_TO_CHECK) ]];
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
       fi;
    done
}

check_for_crashes(){
    while :
    do
        if [ ! -n "$(ps -p $SERVER_PID -o pid=)" ]; then
	    echo "Server crashed."
            cat server.log
            exit -1;
	fi;
        if [ ! -n "$(ps -p $WORKER_PID -o pid=)" ]; then
	    echo "Worker crashed."
            cat worker.log
            exit -1;
	fi;
    done
}

start_server_and_worker(){
  echo "Testing with Shard Instances."

  # Build first to create more predictable run time.
  ./bazel build $BUILDFARM_SERVER_TARGET $BUILDFARM_WORKER_TARGET

  # Start the server.
  ./bazel run $BUILDFARM_SERVER_TARGET -- $BUILDFARM_SERVER_CONFIG > server.log 2>&1 &
  SERVER_PID=$!

  ensure_server_is_up

  # Start the worker.
  ./bazel run $BUILDFARM_WORKER_TARGET -- $BUILDFARM_WORKER_CONFIG > worker.log 2>&1 &
  WORKER_PID=$!
}

init_grpc_parser(){
    if [ "${CACHE_TEST:-false}" = true ]; then
        echo "Fetch tools_remote from git"

        git clone https://github.com/bazelbuild/tools_remote.git
        cd tools_remote;
        git reset --hard $SHA1_TOOLS_REMOTE
        ../bazel build \
            --java_language_version=17 --java_runtime_version=remotejdk_17 \
            --tool_java_language_version=17 --tool_java_runtime_version=remotejdk_17 \
            //:remote_client
        cd /buildfarm;
    fi
}
parse_grpc_logs(){
    if [ "${CACHE_TEST:-false}" = true ]; then
        echo "Parse grpc log"
        ./tools_remote/bazel-bin/remote_client --grpc_log src/test/many/grpc.log printlog --format_json > $GRPC_LOGS1
        ./tools_remote/bazel-bin/remote_client --grpc_log src/test/many/grpc2.log printlog --format_json > $GRPC_LOGS2
        calculate_cache_statistics "$GRPC_LOGS1" "$GRPC_LOGS2"
    fi
}

calculate_cache_statistics(){
    echo "Calculating cache statistics..."
    ./.bazelci/ingest_grpc_logs.py $1 $2
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
