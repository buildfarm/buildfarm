#!/bin/bash
# Delay running a program by sleeping.
# If you are also skipping sleep syscalls, this will result in timeshifting you into the future.
# This can help catch problems by tricking the program into thinking its future time.
sleep $1
"${@: 2}";