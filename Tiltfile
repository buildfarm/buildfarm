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
    'bazel build --javabase=@bazel_tools//tools/jdk:remote_jdk11 //:buildfarm-shard-worker.tar && ' +
    'docker load < bazel-bin/buildfarm-shard-worker.tar && ' +
    'docker tag bazel:buildfarm-shard-worker $EXPECTED_REF'

    ),
  deps=bazel_sourcefile_deps('//:buildfarm-shard-worker.tar')
)
custom_build(
  ref='buildfarm-server-image',
  command=(
    'bazel build --javabase=@bazel_tools//tools/jdk:remote_jdk11 //:buildfarm-server.tar && ' +
    'docker load < bazel-bin/buildfarm-server.tar && ' +
    'docker tag bazel:buildfarm-server $EXPECTED_REF'

    ),
  deps=bazel_sourcefile_deps('//:buildfarm-server.tar')
)

local_resource("unit tests",'bazel test --javabase=@bazel_tools//tools/jdk:remote_jdk11 //src/test/java/...')

# Object definitions for kubernetes.
# Tilt will automatically correlate them to any above docker images.
k8s_yaml(local('bazel run //kubernetes/deployments:kubernetes'))
k8s_yaml(local('bazel run //kubernetes/deployments:server'))
k8s_yaml(local('bazel run //kubernetes/deployments:shard-worker'))
k8s_yaml(local('bazel run //kubernetes/deployments:redis-cluster'))
k8s_yaml(local('bazel run //kubernetes/services:grafana'))
k8s_yaml(local('bazel run //kubernetes/services:redis-cluster'))
k8s_yaml(local('bazel run //kubernetes/services:shard-worker'))
k8s_yaml(local('bazel run //kubernetes/services:open-telemetry'))
k8s_yaml(local('bazel run //kubernetes/services:jaeger'))

# Expose endpoints outside the kubernetes cluster.
k8s_resource('server', port_forwards=[8980,9092], labels="buildfarm-cluster")
k8s_resource('shard-worker', port_forwards=[8981,9091], labels="buildfarm-cluster")
k8s_resource('redis-cluster', port_forwards=6379, labels="buildfarm-cluster")
k8s_resource('otel-agent', labels="tracing")
k8s_resource('otel-collector', port_forwards=[4317,4318], labels="tracing")
k8s_resource('simplest', port_forwards=[14269,16686], labels="tracing")
k8s_resource('kubernetes-dashboard', port_forwards=8443)
k8s_resource('grafana', port_forwards=3000, labels="metrics")
