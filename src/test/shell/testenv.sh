#!/bin/bash
#
# Copyright 2015 The Bazel Authors. All rights reserved.
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
# Testing environment for the Buildfarm integration tests
#
# TODO(bazel-team): This file is currently an append of the old testenv.sh and
# test-setup.sh files. This must be cleaned up eventually.

# TODO(bazel-team): Factor each test suite's is-this-windows setup check to use
# this var instead, or better yet a common $IS_WINDOWS var.
PLATFORM="$(uname -s | tr [:upper:] [:lower:])"

function _log_base() {
  prefix=$1
  shift
  echo >&2 "${prefix}[$(basename "$0") $(date "+%Y-%m-%d %H:%M:%S (%z)")] $*"
}

function log_info() {
  _log_base "INFO" "$@"
}

function log_fatal() {
  _log_base "ERROR" "$@"
  exit 1
}

if ! type rlocation &> /dev/null; then
  log_fatal "rlocation() is undefined"
fi

################### shell/bazel/testenv ##################################
# Setting up the environment for Buildfarm integration tests.
#
[ -z "$TEST_SRCDIR" ] && log_fatal "TEST_SRCDIR not set!"
BAZEL_RUNFILES="$TEST_SRCDIR/build_buildfarm"

#
# Find a random unused TCP port
#
pick_random_unused_tcp_port () {
    perl -MSocket -e '
sub CheckPort {
  my ($port) = @_;
  socket(TCP_SOCK, PF_INET, SOCK_STREAM, getprotobyname("tcp"))
    || die "socket(TCP): $!";
  setsockopt(TCP_SOCK, SOL_SOCKET, SO_REUSEADDR, 1)
    || die "setsockopt(TCP): $!";
  return 0 unless bind(TCP_SOCK, sockaddr_in($port, INADDR_ANY));
  socket(UDP_SOCK, PF_INET, SOCK_DGRAM, getprotobyname("udp"))
    || die "socket(UDP): $!";
  return 0 unless bind(UDP_SOCK, sockaddr_in($port, INADDR_ANY));
  return 1;
}
for (1 .. 128) {
  my ($port) = int(rand() * 27000 + 32760);
  if (CheckPort($port)) {
    print "$port\n";
    exit 0;
  }
}
print "NO_FREE_PORT_FOUND\n";
exit 1;
'
}

################### shell/bazel/test-setup ###############################
# Setup bazel for integration tests
#

# OS X has a limit in the pipe length, so force the root to a shorter one
bazel_root="${TEST_TMPDIR}/root"

# Delete stale installation directory from previously failed tests. On Windows
# we regularly get the same TEST_TMPDIR but a failed test may only partially
# clean it up, and the next time the test runs, Bazel reports a corrupt
# installation error. See https://github.com/bazelbuild/bazel/issues/3618
rm -rf "${bazel_root}"
mkdir -p "${bazel_root}"

# Runs a command, retrying if needed for a fixed timeout.
#
# Necessary to use it on Windows, typically when deleting directory trees,
# because the OS cannot delete open files, which we attempt to do when deleting
# workspaces where a Bazel server is still in the middle of shutting down.
# (Because "bazel shutdown" returns sooner than the server actually shuts down.)
function try_with_timeout() {
  for i in {1..120}; do
    if $* ; then
      break
    fi
    if (( i == 10 )) || (( i == 30 )) || (( i == 60 )) ; then
      log_info "try_with_timeout($*): no success after $i seconds" \
               "(timeout in $((120-i)) seconds)"
    fi
    sleep 1
  done
}

function setup_bazelrc() {
  cat >$TEST_TMPDIR/bazelrc <<EOF
# Set the user root properly for this test invocation.
startup --output_user_root=${bazel_root}

# Print all progress messages because we regularly grep the output in tests.
common --show_progress_rate_limit=-1

# Disable terminal-specific features.
common --color=no --curses=no

# TODO(#7899): Remove once we flip the flag default.
build --incompatible_use_python_toolchains=true

build --incompatible_skip_genfiles_symlink=false

${EXTRA_BAZELRC:-}
EOF

  if [[ -n ${REPOSITORY_CACHE:-} ]]; then
    echo "testenv.sh: Using repository cache at $REPOSITORY_CACHE."
    cat >>$TEST_TMPDIR/bazelrc <<EOF
sync --repository_cache=$REPOSITORY_CACHE --experimental_repository_cache_hardlinks
fetch --repository_cache=$REPOSITORY_CACHE --experimental_repository_cache_hardlinks
build --repository_cache=$REPOSITORY_CACHE --experimental_repository_cache_hardlinks
query --repository_cache=$REPOSITORY_CACHE --experimental_repository_cache_hardlinks
EOF
  fi

  if [[ -n ${INSTALL_BASE:-} ]]; then
    echo "testenv.sh: Using shared install base at $INSTALL_BASE."
    echo "startup --install_base=$INSTALL_BASE" >> $TEST_TMPDIR/bazelrc
  fi
}

function add_rules_cc_to_workspace() {
  cat >> "$1"<<EOF
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "rules_cc",
    sha256 = "36fa66d4d49debd71d05fba55c1353b522e8caef4a20f8080a3d17cdda001d89",
    strip_prefix = "rules_cc-0d5f3f2768c6ca2faca0079a997a97ce22997a0c",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_cc/archive/0d5f3f2768c6ca2faca0079a997a97ce22997a0c.zip",
        "https://github.com/bazelbuild/rules_cc/archive/0d5f3f2768c6ca2faca0079a997a97ce22997a0c.zip",
    ],
)
EOF
}

function add_rules_java_to_workspace() {
  cat >> "$1"<<EOF
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "rules_java",
    sha256 = "bc81f1ba47ef5cc68ad32225c3d0e70b8c6f6077663835438da8d5733f917598",
    strip_prefix = "rules_java-7cf3cefd652008d0a64a419c34c13bdca6c8f178",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_java/archive/7cf3cefd652008d0a64a419c34c13bdca6c8f178.zip",
        "https://github.com/bazelbuild/rules_java/archive/7cf3cefd652008d0a64a419c34c13bdca6c8f178.zip",
    ],
)
EOF
}

# TODO(https://github.com/bazelbuild/bazel/issues/8986): Build this dynamically
# from //WORKSPACE
function add_rules_pkg_to_workspace() {
  cat >> "$1"<<EOF
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "rules_pkg",
    sha256 = "5bdc04987af79bd27bc5b00fe30f59a858f77ffa0bd2d8143d5b31ad8b1bd71c",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_pkg/rules_pkg-0.2.0.tar.gz",
        "https://github.com/bazelbuild/rules_pkg/releases/download/0.2.0/rules_pkg-0.2.0.tar.gz",
    ],
)
EOF
}

function add_rules_proto_to_workspace() {
  cat >> "$1"<<EOF
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "rules_proto",
    sha256 = "602e7161d9195e50246177e7c55b2f39950a9cf7366f74ed5f22fd45750cd208",
    strip_prefix = "rules_proto-97d8af4dc474595af3900dd85cb3a29ad28cc313",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_proto/archive/97d8af4dc474595af3900dd85cb3a29ad28cc313.tar.gz",
        "https://github.com/bazelbuild/rules_proto/archive/97d8af4dc474595af3900dd85cb3a29ad28cc313.tar.gz",
    ],
)
EOF
}

function create_workspace_with_default_repos() {
  touch "$1"
  add_rules_cc_to_workspace "$1"
  add_rules_java_to_workspace "$1"
  add_rules_pkg_to_workspace "$1"
  add_rules_proto_to_workspace "$1"
  echo "$1"
}

# Write the default WORKSPACE file, wiping out any custom WORKSPACE setup.
function write_workspace_file() {
  cat > WORKSPACE << EOF
workspace(name = '$WORKSPACE_NAME')
EOF
  add_rules_cc_to_workspace "WORKSPACE"
  add_rules_java_to_workspace "WORKSPACE"
  add_rules_pkg_to_workspace "WORKSPACE"
  add_rules_proto_to_workspace "WORKSPACE"
}

workspaces=()
# Set-up a new, clean workspace with only the tools installed.
function create_new_workspace() {
  new_workspace_dir=${1:-$(mktemp -d ${TEST_TMPDIR}/workspace.XXXXXXXX)}
  try_with_timeout rm -fr ${new_workspace_dir}
  mkdir -p ${new_workspace_dir}
  workspaces+=(${new_workspace_dir})
  cd ${new_workspace_dir}
  mkdir tools

  write_workspace_file
}


# Set-up a clean default workspace.
function setup_clean_workspace() {
  export WORKSPACE_DIR=${TEST_TMPDIR}/workspace
  log_info "setting up client in ${WORKSPACE_DIR}" >> $TEST_log
  try_with_timeout rm -fr ${WORKSPACE_DIR}
  create_new_workspace ${WORKSPACE_DIR}
  [ "${new_workspace_dir}" = "${WORKSPACE_DIR}" ] \
    || log_fatal "Failed to create workspace"

  if [[ $PLATFORM =~ msys ]]; then
    export BAZEL_SH="$(cygpath --windows /bin/bash)"
  fi
}

# Clean up all files that are not in tools directories, to restart
# from a clean workspace
function cleanup_workspace() {
  if [ -d "${WORKSPACE_DIR:-}" ]; then
    log_info "Cleaning up workspace" >> $TEST_log
    cd ${WORKSPACE_DIR}
    bazel clean >> "$TEST_log" 2>&1

    for i in *; do
      try_with_timeout rm -fr "$i"
    done
    write_workspace_file
  fi
  for i in "${workspaces[@]}"; do
    if [ "$i" != "${WORKSPACE_DIR:-}" ]; then
      try_with_timeout rm -fr $i
    fi
  done
  workspaces=()
}

function testenv_tear_down() {
  cleanup_workspace
}

# This is called by unittest.bash upon eventual exit of the test suite.
function cleanup() {
  if [ -d "${WORKSPACE_DIR:-}" ]; then
    # Try to shutdown Bazel at the end to prevent a "Cannot delete path" error
    # on Windows when the outer Bazel tries to delete $TEST_TMPDIR.
    cd "${WORKSPACE_DIR}"
    try_with_timeout bazel shutdown || true
  fi
}

#
# Simples assert to make the tests more readable
#
function assert_build() {
  bazel build -s --verbose_failures $* || fail "Failed to build $*"
}

function assert_build_output() {
  local OUTPUT=$1
  shift
  assert_build "$*"
  test -f "$OUTPUT" || fail "Output $OUTPUT not found for target $*"
}

function assert_build_fails() {
  bazel build -s $1 >> $TEST_log 2>&1 \
    && fail "Test $1 succeed while expecting failure" \
    || true
  if [ -n "${2:-}" ]; then
    expect_log "$2"
  fi
}

function assert_test_ok() {
  bazel test --test_output=errors $* >> $TEST_log 2>&1 \
    || fail "Test $1 failed while expecting success"
}

function assert_test_fails() {
  bazel test --test_output=errors $* >> $TEST_log 2>&1 \
    && fail "Test $* succeed while expecting failure" \
    || true
  expect_log "$1.*FAILED"
}

function assert_binary_run() {
  $1 >> $TEST_log 2>&1 || fail "Failed to run $1"
  [ -z "${2:-}" ] || expect_log "$2"
}

function assert_bazel_run() {
  bazel run $1 >> $TEST_log 2>&1 || fail "Failed to run $1"
    [ -z "${2:-}" ] || expect_log "$2"

  assert_binary_run "./bazel-bin/$(echo "$1" | sed 's|^//||' | sed 's|:|/|')" "${2:-}"
}

setup_bazelrc

################### shell/integration/testenv ############################
# Setting up the environment for our legacy integration tests.
#
WORKSPACE_NAME=main
bazelrc=$TEST_TMPDIR/bazelrc

function put_bazel_on_path() {
  # do nothing as test-setup already does that
  true
}

function write_default_bazelrc() {
  setup_bazelrc
}

function add_to_bazelrc() {
  echo "$@" >> $bazelrc
}

function create_and_cd_client() {
  setup_clean_workspace
  touch .bazelrc
}

################### Extra ############################

# Functions that need to be called before each test.

create_and_cd_client
