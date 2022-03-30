#!/bin/bash
# Typically our redis implementations are mocked.
# However this runs unit tests that interact directly with redis.

# Run redis container
docker run -d --rm --name buildfarm-redis --network host redis:5.0.9 --bind localhost

# Run tests that rely on redis
./bazelw test --build_tests_only --define native-redis=true src/test/java/...