#!/bin/bash
#
# Copyright 2019 The Bazel Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Tests remote execution and caching.
#

# Load the test setup defined in the parent directory
CURRENT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${CURRENT_DIR}/../integration_test_setup.sh" \
  || { echo "integration_test_setup.sh not found!" >&2; exit 1; }

function set_up() {
  server_pid_file=$(mktemp -u "${TEST_TMPDIR}/remote.XXXXXXXX")
  server_config_file=$(mktemp -u "${TEST_TMPDIR}/remote.XXXXXXXX")
  attempts=1
  while [ $attempts -le 5 ]; do
    (( attempts++ ))
    server_port=$(pick_random_unused_tcp_port) || fail "no port found"
    cat >$server_config_file <<EOS
instances {
  name: "integration_test_instance"
  digest_function: SHA256
  memory_instance_config: {
    list_operations_default_page_size: 1024
    list_operations_max_page_size: 16384
    tree_default_page_size: 1024
    tree_max_page_size: 16384
    operation_poll_timeout: {
      seconds: 30
      nanos: 0
    }
    operation_completed_delay: {
      seconds: 10
      nanos: 0
    }
    cas_config: {
      memory: {
        max_size_bytes: 1073741824 # 1024 * 1024 * 1024
      }
    }
    action_cache_config: {
      delegate_cas: {}
    }
    default_action_timeout: {
      seconds: 600
      nanos: 0
    }
    maximum_action_timeout: {
      seconds: 3600
      nanos: 0
    }
  }
}
port: ${server_port}
default_instance_name: "integration_test_instance"
EOS
    "${BAZEL_RUNFILES}/src/main/java/build/buildfarm/buildfarm-server" \
        $server_config_file \
        --pid_file="${server_pid_file}" >& $TEST_log &
    local wait_seconds=0
    until [ -s "${server_pid_file}" ] || [ "$wait_seconds" -eq 15 ]; do
      sleep 1
      ((wait_seconds++)) || true
    done
    if [ -s "${server_pid_file}" ]; then
      break
    fi
  done
  if [ ! -s "${server_pid_file}" ]; then
    fail "Timed out waiting for remote server to start."
  fi

  worker_root=$(mktemp -d "${TEST_TMPDIR}/remote.XXXXXXXX")
  worker_pid_file=$(mktemp -u "${TEST_TMPDIR}/remote.XXXXXXXX")
  worker_config_file=$(mktemp -u "${TEST_TMPDIR}/remote.XXXXXXXX")
  attempts=1
  while [ $attempts -le 5 ]; do
    (( attempts++ ))

    if [[ "$PLATFORM" == "darwin" ]]; then
      true; # TODO plug in SDKROOT and DEVELOPER_DIR execution_policies
    fi

    cat >$worker_config_file <<EOS
digest_function: SHA256
operation_queue: {
  target: "localhost:${server_port}"
  instance_name: "integration_test_instance"
  deadline_after_seconds: 60
}
content_addressable_storage: {
  target: "localhost:${server_port}"
  instance_name: "integration_test_instance"
  deadline_after_seconds: 60
}
action_cache: {
  target: "localhost:${server_port}"
  instance_name: "integration_test_instance"
  deadline_after_seconds: 60
}
root: "${worker_root}"
cas_cache_directory: "cache"
inline_content_limit: 1048567 # 1024 * 1024
stream_stdout: true
stdout_cas_policy: ALWAYS_INSERT
stream_stderr: true
stderr_cas_policy: ALWAYS_INSERT
file_cas_policy: ALWAYS_INSERT
tree_page_size: 0
operation_poll_period: {
  seconds: 1
  nanos: 0
}
cas_cache_max_size_bytes: 2147483648 # 2 * 1024 * 1024 * 1024
cas_cache_max_entry_size_bytes: 2147483648 # 2 * 1024 * 1024 * 1024
execute_stage_width: 1
input_fetch_stage_width: 1
default_action_timeout: {
  seconds: 600
  nanos: 0
}
maximum_action_timeout: {
  seconds: 3600
  nanos: 0
}
EOS
    "${BAZEL_RUNFILES}/src/main/java/build/buildfarm/buildfarm-operationqueue-worker" \
        $worker_config_file \
        --pid_file="${worker_pid_file}" >& $TEST_log &
    local wait_seconds=0
    until [ -s "${worker_pid_file}" ] || [ "$wait_seconds" -eq 15 ]; do
      sleep 1
      ((wait_seconds++)) || true
    done
    if [ -s "${worker_pid_file}" ]; then
      break
    fi
  done
  if [ ! -s "${worker_pid_file}" ]; then
    fail "Timed out waiting for remote worker to start."
  fi
}

function tear_down() {
  bazel clean >& $TEST_log
  if [ -s "${server_pid_file}" ]; then
    local server_pid=$(cat "${server_pid_file}")
    kill "${server_pid}" || true
  fi
  if [ -s "${worker_pid_file}" ]; then
    local worker_pid=$(cat "${worker_pid_file}")
    kill "${worker_pid}" || true
  fi
  rm -rf "${server_pid_file}"
  rm -rf "${worker_pid_file}"
  rm -rf "${worker_root}"
}

function test_cc_binary() {
  mkdir -p a
  cat > a/BUILD <<EOF
cc_binary(
name = 'test',
srcs = [ 'test.cc' ],
)
EOF
  cat > a/test.cc <<EOF
#include <iostream>
int main() { std::cout << "Hello world!" << std::endl; return 0; }
EOF
  bazel build //a:test >& $TEST_log \
    || fail "Failed to build //a:test without remote execution"
  cp -f bazel-bin/a/test ${TEST_TMPDIR}/test_expected

  bazel clean >& $TEST_log
  bazel build \
      --remote_executor=grpc://localhost:${server_port} \
      //a:test >& $TEST_log \
      || fail "Failed to build //a:test with remote execution"
  expect_log "2 processes: 2 remote"
  diff bazel-bin/a/test ${TEST_TMPDIR}/test_expected \
      || fail "Remote execution generated different result"
}

run_suite "Buildfarm OperationQueue Remote Execution Tests"
