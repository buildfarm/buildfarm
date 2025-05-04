#!/bin/bash

set -eo pipefail

# Run from the root of repository.
# This script will remove any unused java dependencies
# Use the flag --check if you want the script to fail when the dependencies not correct.

DEPS_TOOL_URL=https://github.com/bazelbuild/buildtools/releases/download/4.0.1/unused_deps-linux-amd64
LOCAL_DEPS_TOOL="./unused_deps"

BUILDOZER_URL=https://github.com/bazelbuild/buildtools/releases/download/4.0.1/buildozer-linux-amd64
LOCAL_BUILDOZER_TOOL="./buildozer"

# The "unused deps" tool is not perfect.  It might try to remove a
# dependency that is needed at runtime.  If that is the case, the dependency
# can be added to this list.  All buildozer commands referencing this dependency
# will not be executed.
keep_depenencies=( "//src/test/java/build/buildfarm:test_runner" )

# Print an error such that it will surface in the context of buildkite
print_error () {
    >&2 echo "$1"
    if [ -v BUILDKITE ] ; then
        buildkite-agent annotate "$1" --style 'error' --context 'ctx-error'
    fi
}

# Download the tools if we don't already have them.
download_missing_tools () {
    if [ ! -f "$LOCAL_DEPS_TOOL" ] ; then
        wget -O $LOCAL_DEPS_TOOL $DEPS_TOOL_URL
        chmod +x $LOCAL_DEPS_TOOL
    fi

    if [ ! -f "$LOCAL_BUILDOZER_TOOL" ] ; then
        wget -O $LOCAL_BUILDOZER_TOOL $BUILDOZER_URL
        chmod +x $LOCAL_BUILDOZER_TOOL
    fi
}

# Run the unused_deps tool to create a list of buildozer commands
# Certain commands are removed based on a configuration to avoid runtime issues.
create_buildozer_commands () {
    $LOCAL_DEPS_TOOL //src/... > /tmp/unused_deps_results;
    grep "^buildozer" /tmp/unused_deps_results > /tmp/unused_deps_filtered;
    for i in "${keep_depenencies[@]}"
    do
        escaped_keyword=$(printf '%s\n' "$i" | sed -e 's/[]\/$*.^[]/\\&/g');
        sed -i '/'$escaped_keyword'/d' /tmp/unused_deps_filtered;
    done
    awk '{print "./" $0}' /tmp/unused_deps_filtered > /tmp/buildozer_commands.sh
    chmod +x /tmp/buildozer_commands.sh
}

download_missing_tools;
create_buildozer_commands;

# Check whether any dependencies changes need to be made.
# This is intended to be done by the CI.
if [[ "$@" == "--check" ]]
then
    if ! grep -q '[^[:space:]]' "/tmp/buildozer_commands.sh"; then
        echo "Dependencies are correct."
        exit 0
    else
        print_error 'Run ./.bazelci/unused_deps.sh to resolve dependency issues.'
        echo "The following commands need run:"
        cat /tmp/buildozer_commands.sh
        exit 1
    fi
fi

# Run all of the buildozer commands to fix the dependencies
/tmp/buildozer_commands.sh

