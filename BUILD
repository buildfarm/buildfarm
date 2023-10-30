load("@com_github_bazelbuild_buildtools//buildifier:def.bzl", "buildifier")
load("@io_bazel_rules_docker//java:image.bzl", "java_image")
load("@io_bazel_rules_docker//docker/package_managers:download_pkgs.bzl", "download_pkgs")
load("@io_bazel_rules_docker//docker/package_managers:install_pkgs.bzl", "install_pkgs")
load("@io_bazel_rules_docker//container:container.bzl", "container_image")
load("@rules_oss_audit//oss_audit:java/oss_audit.bzl", "oss_audit")
load("//:jvm_flags.bzl", "server_jvm_flags", "worker_jvm_flags")

package(default_visibility = ["//visibility:public"])

# Made available for formatting
buildifier(
    name = "buildifier",
)

genrule(
    name = "opentelemetry-javaagent",
    srcs = ["@opentelemetry//jar"],
    outs = ["opentelemetry-javaagent.jar"],
    cmd = "cp $< $@;",
)

java_library(
    name = "telemetry_tools",
    data = [
        ":opentelemetry-javaagent",
    ],
)

# == Docker Image Creation ==
# When deploying buildfarm, you may want to include additional dependencies within your deployment.
# These dependencies can enable features related to the observability and runtime of the system.
# For example, "debgging tools", "introspection tools", and "exeution wrappers" are examples of dependencies
# that many need included within deployed containers.  This BUILD file creates docker images that bundle
# additional dependencies alongside the buildfarm agents.

# Docker images for buildfarm components
java_image(
    name = "buildfarm-server",
    args = ["/app/build_buildfarm/examples/config.minimal.yml"],
    base = "@ubuntu-mantic//image",
    classpath_resources = [
        "//src/main/java/build/buildfarm:configs",
    ],
    data = [
        "//examples:example_configs",
        "//src/main/java/build/buildfarm:configs",
    ],
    jvm_flags = server_jvm_flags(),
    main_class = "build.buildfarm.server.BuildFarmServer",
    tags = ["container"],
    runtime_deps = [
        ":telemetry_tools",
        "//src/main/java/build/buildfarm/server",
    ],
)

oss_audit(
    name = "buildfarm-server-audit",
    src = "//src/main/java/build/buildfarm:buildfarm-server",
    tags = ["audit"],
)

# A worker image may need additional packages installed that are not in the base image.
# We use download/install rules to extend an upstream image.
# Download cgroup-tools so that the worker is able to restrict actions via control groups.
download_pkgs(
    name = "worker_pkgs",
    image_tar = "@ubuntu-mantic//image",
    packages = ["cgroup-tools"],
    tags = ["container"],
)

install_pkgs(
    name = "worker_pkgs_image",
    image_tar = "@ubuntu-mantic//image",
    installables_tar = ":worker_pkgs.tar",
    installation_cleanup_commands = "rm -rf /var/lib/apt/lists/*",
    output_image_name = "worker_pkgs_image",
    tags = ["container"],
)

# This becomes the new base image when creating worker images.
container_image(
    name = "worker_pkgs_image_wrapper",
    base = ":worker_pkgs_image.tar",
    tags = ["container"],
)

java_image(
    name = "buildfarm-shard-worker",
    args = ["/app/build_buildfarm/examples/config.minimal.yml"],
    base = ":worker_pkgs_image_wrapper",
    classpath_resources = [
        "//src/main/java/build/buildfarm:configs",
    ],
    data = [
        "//examples:example_configs",
        "//src/main/java/build/buildfarm:configs",
    ],
    jvm_flags = worker_jvm_flags(),
    main_class = "build.buildfarm.worker.shard.Worker",
    tags = ["container"],
    runtime_deps = [
        ":telemetry_tools",
        "//src/execution_wrappers",
        "//src/main/java/build/buildfarm/worker/shard",
    ],
)

oss_audit(
    name = "buildfarm-shard-worker-audit",
    src = "//src/main/java/build/buildfarm:buildfarm-shard-worker",
    tags = ["audit"],
)
