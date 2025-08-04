#!/bin/bash
# Copyright 2021-2025 The Buildfarm Authors. All rights reserved.
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

# This is to run buildfarm's unit tests from within a docker container

# Build a container for unit tests and run them
cp `which bazel` bazel
docker build -t buildfarm .
docker run buildfarm /bin/bash -c "cd buildfarm; ./bazel test --build_tests_only --test_tag_filters=-container,-integration,-redis ..."