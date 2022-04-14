#!/bin/bash
# Typically our redis implementations are mocked.
# However this runs unit tests that interact directly with redis.

# Run redis container
docker run -d --rm --name buildfarm-redis --network host redis:5.0.9 --bind localhost

# Run tests that rely on redis
bazel test --build_tests_only --test_tag_filters=redis src/test/java/...
