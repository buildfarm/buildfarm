#!/bin/bash
# Test bazel targets with buildfarm
cd src/test/abseil;
../../../bazel test --jobs=25 --test_tag_filters=-benchmark --remote_executor=grpc://localhost:8980 @com_google_absl//... -- -@com_google_absl//absl/time/...