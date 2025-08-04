#!/usr/bin/env bash
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

# Delay running a program by sleeping.
# If you are also skipping sleep syscalls, this will result in timeshifting you into the future.
# This can help catch problems by tricking the program into thinking its future time.
# WARNING: this script is meant to work in tandem with the skip_sleep wrapper to jump forward in time.
# If you're not using them together, delay will cause dramatic impact on the schedulability of an action (in the best case its expiration).
sleep $1
"${@: 2}";
