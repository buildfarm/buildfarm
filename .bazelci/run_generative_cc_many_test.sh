#!/bin/bash
# Build bazel targets with buildfarm
cd src/test/many;
MANY_CC_BINARIES=50 MANY_CC_LIBRARIES=2 MANY_CC_LIBRARY_SOURCES=1 ./../../../bazelw build :cc --remote_executor=grpc://localhost:8980