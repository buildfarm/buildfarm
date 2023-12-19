# -*- mode: Python -*-

# This Tiltfile defines a complete dev environment for buildfarm.
# It automates all the steps from a code change to a new cluster:
# watching files, building container images, and redeploying parts of the cluster.
# Think bazel-watcher + docker build + kubectl apply or docker-compose up.

# Install: https://docs.tilt.dev/
# Run `tilt up`.


# Dependency for live-reload
load('./bazel.Tiltfile', 'bazel_sourcefile_deps')

# Overrides Tilt telemetry.
# By default, Tilt does not send telemetry.
# After you successfully run a Tiltfile, the Tilt web UI will nudge you to opt in or opt out of telemetry.
# This avoids that from happening.
analytics_settings(False)

# This is provided as an option because it is often slow to compute file changes this way.
USE_BAZEL_QUERY_TO_REBUILD=False

def worker_deps():
  if USE_BAZEL_QUERY_TO_REBUILD:
    return bazel_sourcefile_deps('//:buildfarm-shard-worker.tar')
  return ()
  
def server_deps():
  if USE_BAZEL_QUERY_TO_REBUILD:
    return bazel_sourcefile_deps('//:buildfarm-shard-worker.tar')
  return ()

# Inform tilt about the custom images built within the repository.
# When you change code, these images will be re-built and re-deployed.
custom_build(
  ref='bazelbuild/buildfarm-worker',
  command=(
    'bazelisk build --javabase=@bazel_tools//tools/jdk:remote_jdk11 //:buildfarm-shard-worker.tar && ' +
    'docker load < bazel-bin/buildfarm-shard-worker.tar && ' +
    'docker tag bazel:buildfarm-shard-worker $EXPECTED_REF'

    ),
    deps = worker_deps()
)
custom_build(
  ref='bazelbuild/buildfarm-server',
  command=(
    'bazelisk build --javabase=@bazel_tools//tools/jdk:remote_jdk11 //:buildfarm-server.tar && ' +
    'docker load < bazel-bin/buildfarm-server.tar && ' +
    'docker tag bazel:buildfarm-server $EXPECTED_REF'

    ),
    deps = server_deps()
)

local_resource("unit tests",'bazelisk test --javabase=@bazel_tools//tools/jdk:remote_jdk11 //src/test/java/...')

# Object definitions for kubernetes.
# Tilt will automatically correlate them to any above docker images.
k8s_yaml(helm('kubernetes/helm-charts/buildfarm'))
