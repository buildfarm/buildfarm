# -*- mode: Python -*-

# This Tiltfile defines a complete dev environment for buildfarm.
# It automates all the steps from a code change to a new process:
# watching files, building container images, and bringing your environment up-to-date.
# Think docker build && kubectl apply or docker-compose up.

# Install: https://docs.tilt.dev/
# Run `tilt up`.


# Dependency for live-reload
load('./bazel.Tiltfile', 'bazel_sourcefile_deps')

# Overrides Tilt telemetry.
# By default, Tilt does not send telemetry.
# After you successfully run a Tiltfile, the Tilt web UI will nudge you to opt in or opt out of telemetry.
# This avoids that from happening.
analytics_settings(False) 

# Inform tilt about the custom images built within the repository.
# When you change code, these images will be re-built and re-deployed.
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

local_resource("unit tests",'./bazelw test --javabase=@bazel_tools//tools/jdk:remote_jdk11 //src/test/...')

# Object definitions for kubernetes.
# Tilt will automatically correlate them to any above docker images.
k8s_yaml(local('./bazelw run //kubernetes/deployments:kubernetes'))
k8s_yaml(local('./bazelw run //kubernetes/deployments:server'))
k8s_yaml(local('./bazelw run //kubernetes/deployments:shard-worker'))
k8s_yaml(local('./bazelw run //kubernetes/deployments:redis-cluster'))
k8s_yaml(local('./bazelw run //kubernetes/services:redis-cluster'))
k8s_yaml(local('./bazelw run //kubernetes/services:shard-worker'))

# Expose endpoints outside the kubernetes cluster.
k8s_resource('kubernetes-dashboard', port_forwards=8443)
k8s_resource('shard-worker', port_forwards=8981)
k8s_resource('server', port_forwards=8980)
k8s_resource('redis-cluster', port_forwards=6379)
