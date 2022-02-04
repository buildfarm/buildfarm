#!/bin/bash
# Test bazel targets with buildfarm
cd src/test/abseil;
./../../../bazelw test --test_tag_filters=-benchmark -k --remote_executor=grpc://localhost:8980 @com_google_absl//... -- -@com_google_absl//absl/time/...