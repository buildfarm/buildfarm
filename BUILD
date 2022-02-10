load("@com_github_bazelbuild_buildtools//buildifier:def.bzl", "buildifier")
load("@io_bazel_rules_docker//java:image.bzl", "java_image")
load("@io_bazel_rules_docker//docker/package_managers:download_pkgs.bzl", "download_pkgs")
load("@io_bazel_rules_docker//docker/package_managers:install_pkgs.bzl", "install_pkgs")
load("@io_bazel_rules_docker//container:container.bzl", "container_image")

package(default_visibility = ["//visibility:public"])

buildifier(
    name = "buildifier",
)

# These are execution wrappers that buildfarm may choose to use when executing actions.
# For their availability on a worker, they should be provided to a java_image as a "runtime_dep".
# The relevant configuration for workers is the "execution policy".
# That is where these binaries can be used and stacked.
# Buildfarm may also choose different execution wrappers dynamically based on exec_properties.
# Be aware that the process-wrapper and linux-sandbox come from bazel itself.
# Therefore, users may want to ensure that the same bazel version is sourced here as is used locally.
java_library(
    name = "execution_wrappers",
    runtime_deps = [
        ":as-nobody",
        ":delay",
        ":linux-sandbox.binary",
        ":process-wrapper.binary",
        ":skip_sleep.binary",
        ":skip_sleep.preload",
        ":tini.binary",
    ],
)

genrule(
    name = "process-wrapper.binary",
    srcs = ["@bazel//src/main/tools:process-wrapper"],
    outs = ["process-wrapper"],
    cmd = "cp $< $@;",
)

genrule(
    name = "linux-sandbox.binary",
    srcs = ["@bazel//src/main/tools:linux-sandbox"],
    outs = ["linux-sandbox"],
    cmd = "cp $< $@;",
)

genrule(
    name = "tini.binary",
    srcs = ["@tini//file"],
    outs = ["tini"],
    cmd = "cp $< $@ && chmod +x $@",
)

cc_binary(
    name = "as-nobody",
    srcs = select({
        "//config:windows": ["as-nobody-windows.c"],
        "//conditions:default": ["as-nobody.c"],
    }),
)

genrule(
    name = "skip_sleep.binary",
    srcs = ["@skip_sleep"],
    outs = ["skip_sleep"],
    cmd = "cp $< $@;",
)

genrule(
    name = "skip_sleep.preload",
    srcs = ["@skip_sleep//:skip_sleep_preload"],
    outs = ["skip_sleep_preload.so"],
    cmd = "cp $< $@;",
)

# The delay wrapper is only intended to be used with the "skip_sleep" wrapper.
sh_binary(
    name = "delay",
    srcs = ["delay.sh"],
)

# Docker images for buildfarm components
java_image(
    name = "buildfarm-server",
    args = ["/app/build_buildfarm/examples/shard-server.config.example"],
    base = "@amazon_corretto_java_image_base//image",
    classpath_resources = [
        "//src/main/java/build/buildfarm:configs",
    ],
    data = [
        "//examples:example_configs",
        "//src/main/java/build/buildfarm:configs",
    ],
    jvm_flags = [
        "-Djava.util.logging.config.file=/app/build_buildfarm/src/main/java/build/buildfarm/logging.properties",
    ],
    main_class = "build.buildfarm.server.BuildFarmServer",
    tags = ["container"],
    runtime_deps = [
        "//src/main/java/build/buildfarm/server",
    ],
)

# A worker image may need additional packages installed that are not in the base image.
# We use download/install rules to extend an upstream image.
# Download cgroup-tools so that the worker is able to restrict actions via control groups.
download_pkgs(
    name = "worker_pkgs",
    image_tar = "@ubuntu-bionic//image",
    packages = ["cgroup-tools"],
    tags = ["container"],
)

install_pkgs(
    name = "worker_pkgs_image",
    image_tar = "@ubuntu-bionic//image",
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
    args = ["/app/build_buildfarm/examples/shard-worker.config.example"],
    base = ":worker_pkgs_image_wrapper",
    classpath_resources = [
        "//src/main/java/build/buildfarm:configs",
    ],
    data = [
        "//examples:example_configs",
        "//src/main/java/build/buildfarm:configs",
    ],
    jvm_flags = [
        "-Djava.util.logging.config.file=/app/build_buildfarm/src/main/java/build/buildfarm/logging.properties",
    ],
    main_class = "build.buildfarm.worker.shard.Worker",
    tags = ["container"],
    runtime_deps = [
        ":execution_wrappers",
        "//src/main/java/build/buildfarm/worker/shard",
    ],
)
