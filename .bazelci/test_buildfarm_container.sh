#!/bin/bash
# This script is to be called within a built container reflecting the changes of a PR
# We start the server and the worker, and test that they can complete builds for a bazel client.
cd buildfarm;

# Start the server.
./bazelw run //src/main/java/build/buildfarm:buildfarm-server -- /buildfarm/examples/shard-server.config.example > server.log 2>&1 &
SERVER_PID=$!

# Start the worker.
./bazelw run //src/main/java/build/buildfarm:buildfarm-shard-worker -- /buildfarm/examples/shard-worker.config.example > worker.log 2>&1 &
WORKER_PID=$!

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

# Show startup logs
echo "Server log:"
cat server.log
echo "Worker log:"
cat server.log

# Build bazel targets with buildfarm
cd src/test/many;
MANY_CC_BINARIES=50 MANY_CC_LIBRARIES=2 MANY_CC_LIBRARY_SOURCES=1 ./../../../bazelw build :cc --remote_executor=grpc://localhost:8980