#!/bin/bash
# Run from the root of repository.
# This script will remove any unused java dependencies

DEPS_TOOL_URL=https://github.com/bazelbuild/buildtools/releases/download/4.0.1/unused_deps-linux-amd64
LOCAL_DEPS_TOOL="unused_deps"

BUILDOZER_URL=https://github.com/bazelbuild/buildtools/releases/download/4.0.1/buildozer-linux-amd64
LOCAL_BUILDOZER_TOOL="buildozer"

 # Download the tools if we don't already have them.
if [ ! -f "$LOCAL_DEPS_TOOL" ] ; then
    wget -O $LOCAL_DEPS_TOOL $DEPS_TOOL_URL
    chmod +x $LOCAL_DEPS_TOOL
fi

if [ ! -f "$LOCAL_BUILDOZER_TOOL" ] ; then
    wget -O $LOCAL_BUILDOZER_TOOL $BUILDOZER_URL
    chmod +x $LOCAL_BUILDOZER_TOOL
fi

# Run the unused_deps tool to create a list of buildozer commands
$LOCAL_DEPS_TOOL //src/... > /tmp/unused_deps_results;
grep "^buildozer" /tmp/unused_deps_results > /tmp/unused_deps_commands;

