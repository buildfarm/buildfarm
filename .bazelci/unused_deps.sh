#!/bin/bash
# Run from the root of repository.
# This script will remove any unused java dependencies

TOOL_URL=https://github.com/bazelbuild/buildtools/releases/download/4.0.1/unused_deps-linux-amd64
LOCAL_TOOL="unused_deps"

 # Download the tool if we don't already have it.
if [ ! -f "$LOCAL_TOOL" ] ; then
    wget -O $LOCAL_TOOL $TOOL_URL
    chmod +x $LOCAL_TOOL
fi

$LOCAL_TOOL --help