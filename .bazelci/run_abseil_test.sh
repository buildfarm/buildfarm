#!/bin/bash
# Test bazel targets with buildfarm
cd src/test/abseil;
./../../../bazelw test --test_tag_filters=-benchmark @com_google_absl//... --remote_executor=grpc://localhost:8980 -- -@com_google_absl//absl/time/...