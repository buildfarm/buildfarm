#!/bin/bash
# Typically our redis implementations are mocked.
# However this runs unit tests that interact directly with redis.

# Run redis container
docker run -d --rm --name buildfarm-redis --network host redis:7.2.4 --bind localhost

# Run tests that rely on redis
bazel test --build_tests_only --test_tag_filters=redis src/test/java/...

docker stop buildfarm-redis
