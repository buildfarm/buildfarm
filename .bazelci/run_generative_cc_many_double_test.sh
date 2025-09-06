#!/bin/bash
# Copyright 2022-2025 The Buildfarm Authors. All rights reserved.
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

# Build bazel targets with buildfarm
cd src/test/many;
MANY_CC_BINARIES=50 MANY_CC_LIBRARIES=2 MANY_CC_LIBRARY_SOURCES=1 ../../../bazel build :cc --remote_executor=grpc://localhost:8980 $1
echo "Cleaning bazel..."
../../../bazel clean
MANY_CC_BINARIES=50 MANY_CC_LIBRARIES=2 MANY_CC_LIBRARY_SOURCES=1 ../../../bazel build :cc --remote_executor=grpc://localhost:8980 $2