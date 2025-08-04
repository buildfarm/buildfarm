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

# Typically our redis implementations are mocked.
# However this runs unit tests that interact directly with redis.

# Run redis container
docker run -d --rm --name buildfarm-redis --network host redis:7.2.4 --bind localhost

# Run tests that rely on redis
bazel test --build_tests_only --test_tag_filters=redis src/test/java/...

docker stop buildfarm-redis
