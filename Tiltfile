# -*- mode: Python -*-

# This Tiltfile defines a complete dev environment for buildfarm.
# It automates all the steps from a code change to a new process:
# watching files, building container images, and bringing your environment up-to-date.
# Think docker build && kubectl apply or docker-compose up.

# Install: https://docs.tilt.dev/
# Run `tilt up`.

# Inform tilt about the custom images built within the repository.
# When you change code, these images will be re-built and re-deployed.

load('./bazel.Tiltfile', 'bazel_sourcefile_deps')

custom_build(
  ref='buildfarm-shard-worker-image',
  command=(
    './bazelw build --javabase=@bazel_tools//tools/jdk:remote_jdk11 //:buildfarm-shard-worker.tar && ' +
    'docker load < bazel-bin/buildfarm-shard-worker.tar && ' +
    'docker tag bazel:buildfarm-shard-worker $EXPECTED_REF'

    ),
  deps=bazel_sourcefile_deps('//:buildfarm-shard-worker.tar')
)
custom_build(
  ref='buildfarm-server-image',
  command=(
    './bazelw build --javabase=@bazel_tools//tools/jdk:remote_jdk11 //:buildfarm-server.tar && ' +
    'docker load < bazel-bin/buildfarm-server.tar && ' +
    'docker tag bazel:buildfarm-server $EXPECTED_REF'

    ),
  deps=bazel_sourcefile_deps('//:buildfarm-server.tar')
)

# Object definitions for kubernetes.
# Tilt will automatically correlate them to any above docker images.
k8s_yaml(local('./bazelw run //deployments:server'))
k8s_yaml(local('./bazelw run //deployments:shard-worker'))
k8s_yaml(local('./bazelw run //deployments:redis-cluster'))

# Expose a port needed for buildfarm to connect to redis.
k8s_resource('redis-cluster', port_forwards=6379)
