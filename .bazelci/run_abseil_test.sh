#!/bin/bash
# Test bazel targets with buildfarm
cd src/test/abseil;
../../../bazel test --jobs=25 $1 --test_output=errors --incompatible_enable_cc_toolchain_resolution --verbose_failures --verbose_explanations --test_tag_filters=-benchmark --remote_executor=grpc://localhost:8980 @com_google_absl//... -- -@com_google_absl//absl/time/...
