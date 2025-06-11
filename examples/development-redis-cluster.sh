#!/usr/bin/env bash

# First, start six Redis instances using this script:
#   ./development-redis-cluster.sh 0
#   ./development-redis-cluster.sh 1
#   ./development-redis-cluster.sh 2
#   ./development-redis-cluster.sh 3
#   ./development-redis-cluster.sh 4
#   ./development-redis-cluster.sh 5
#
# Then, turn it into a cluster:
#   redis-cli --cluster create 127.0.0.1:6379 127.0.0.1:6380 127.0.0.1:6381 127.0.0.1:6382 127.0.0.1:6383 127.0.0.1:6384 --cluster-replicas 1
#
# Then, check status with:
#   redis-cli cluster nodes
#   redis-cli cluster info
#
# Then, start your buildfarm server:
#   bazel run //src/main/java/build/buildfarm:buildfarm-server $PWD/examples/config.yml
set -eufo pipefail

function print_usage() {
    # Pretty up and print the header comment of this file
    grep -E '^set ' -B1000 "$0" | sed -E 's/^# ?//' | tail -n+3 | sed '$d'
}

if [ -z ${1+x} ]; then
    echo "ERROR: Need one parameter, 0-5"
    echo
    print_usage
    exit 1
fi

if ! echo "0 1 2 3 4 5" | grep -q "$1" ; then
    echo "ERROR: Parameter must be 0-5, was <$1>"
    echo
    print_usage
    exit 1
fi

BASE_PORT=6379

NUMBER="$1"
PORT="$((BASE_PORT + NUMBER))"

DBDIR="/tmp/redistest/$NUMBER"
mkdir -p "$DBDIR"

redis-server - <<EOF | cat
port $PORT
dir $DBDIR
appendonly yes
cluster-enabled yes
always-show-logo no
EOF
