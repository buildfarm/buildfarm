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

# Test bazel targets with buildfarm
cd src/test/abseil;
../../../bazel test --jobs=25 $1 --noenable_bzlmod --test_output=errors --incompatible_enable_cc_toolchain_resolution --verbose_failures --verbose_explanations --test_tag_filters=-benchmark --remote_executor=grpc://localhost:8980 @com_google_absl//... -- -@com_google_absl//absl/time/...
