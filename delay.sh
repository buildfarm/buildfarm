#!/usr/bin/env bash
# Delay running a program by sleeping.
# If you are also skipping sleep syscalls, this will result in timeshifting you into the future.
# This can help catch problems by tricking the program into thinking its future time.
# WARNING: this script is meant to work in tandem with the skip_sleep wrapper to jump forward in time.
# If you're not using them together, delay will cause dramatic impact on the schedulability of an action (in the best case its expiration).
sleep $1
"${@: 2}";
