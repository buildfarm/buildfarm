load("@buildifier_prebuilt//:rules.bzl", "buildifier")
load("@rules_oci//oci:defs.bzl", "oci_image", "oci_image_index", "oci_push", "oci_tarball")
load("@rules_pkg//:pkg.bzl", "pkg_tar")
load("//:jvm_flags.bzl", "server_jvm_flags", "worker_jvm_flags")

package(default_visibility = ["//visibility:public"])

# Made available for formatting
buildifier(
    name = "buildifier",
)

# == Docker Image Creation ==
# When deploying buildfarm, you may want to include additional dependencies within your deployment.
# These dependencies can enable features related to the observability and runtime of the system.
# For example, "debgging tools", "introspection tools", and "exeution wrappers" are examples of dependencies
# that many need included within deployed containers.  This BUILD file creates docker images that bundle
# additional dependencies alongside the buildfarm agents.

# == Execution Wrappers ==
# Execution wrappers are programs that buildfarm chooses to use when running REAPI actions.  They are used for
# both sandboxing, as well as changing runtime behavior of actions.  Buildfarm workers can be configured
# to use execution wrappers directly through a configuration called "execution policy".  Execution wrappers
# can be stacked (i.e. actions can run under multiple wrappers).  Buildfarm may also choose different
# execution wrappers dynamically based on exec_properties.  In order to have them available to the worker, they should
# be provided to a java_image as a "runtime_dep".  Buildfarm workers will warn about any missing execution wrappers
# during startup and what features are unavailable due to their absence.

# == Execution Wrapper Compatibility ==
# "process-wrapper" and "linux-sandbox" are sourced directly from bazel.  Users may want to ensure that the same
# bazel version is used in buildfarm agents as is used by bazel clients.  There has not been any known issues due
# to version mismatch, but we state the possibility here.  Some execution wrappers will not be compatible with all
# operating systems.  We make a best effort and ensure they all work in the below images.
java_library(
    name = "execution_wrappers",
    data = [
        ":as-nobody",
        ":delay",
        ":linux-sandbox.binary",
        ":macos-wrapper",
        ":process-wrapper.binary",
        ":skip_sleep.binary",
        ":skip_sleep.preload",
    ],
)

java_library(
    name = "telemetry_tools",
    data = [
        ":opentelemetry-javaagent",
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

genrule(
    name = "opentelemetry-javaagent",
    srcs = ["@opentelemetry//jar"],
    outs = ["opentelemetry-javaagent.jar"],
    cmd = "cp $< $@;",
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

sh_binary(
    name = "macos-wrapper",
    srcs = ["macos-wrapper.sh"],
)
################################################################################
## rules_oci
################################################################################

ARCH = [
    # "aarch64", # TODO
    "amd64",
]

pkg_tar(
    name = "layer_tini_amd64",
    srcs = ["@tini//file"],
    mode = "0755",
    remap_paths = {"/downloaded": "/tini"},
    tags = ["container"],
)

pkg_tar(
    name = "layer_buildfarm_server",
    srcs = ["//src/main/java/build/buildfarm:buildfarm-server_deploy.jar"],
    package_dir = "app",
    # Put everything in /app
    tags = ["container"],
)

pkg_tar(
    name = "layer_buildfarm_worker",
    srcs = ["//src/main/java/build/buildfarm:buildfarm-shard-worker_deploy.jar"],
    # Put everything in /app
    package_dir = "app",
    tags = ["container"],
)

pkg_tar(
    name = "layer_minimal_config",
    srcs = ["@build_buildfarm//src/main/java/build/buildfarm:configs"],
    package_dir = "app/config",
    tags = ["container"],
)

oci_image(
    name = "buildfarm-server_linux_amd64",
    base = "@amazon_corretto_java_image_base",
    entrypoint = [
        "/app/buildfarm-server_deploy.jar",
    ],
    env = {
        "JAVA_TOOL_OPTIONS": server_jvm_flags(),
        "CONFIG_PATH": "/app/build_buildfarm/config.minimal.yml",
    },
    labels = {
        "org.opencontainers.image.source": "https://github.com/bazelbuild/bazel-buildfarm",
    },
    tags = ["container"],
    tars = [
        # do not sort
        ":layer_minimal_config",
        # TODO :telemetry_tools
        ":layer_buildfarm_server",
    ],
)

oci_image(
    name = "buildfarm-worker_linux_amd64",
    base = "@ubuntu_mantic",
    entrypoint = [
        # do not sort
        "/tini",
        "--",
        "java",
        "-jar",
        "/app/buildfarm-shard-worker_deploy.jar",
    ],
    env = {
        "JAVA_TOOL_OPTIONS": worker_jvm_flags(),
        "CONFIG_PATH": "/app/build_buildfarm/config.minimal.yml",
    },
    labels = {
        "org.opencontainers.image.source": "https://github.com/bazelbuild/bazel-buildfarm",
    },
    tags = ["container"],
    tars = [
        # do not sort
        ":layer_minimal_config",
        ":layer_tini_amd64",
        # TODO execution wrappers
        # TODO :telemetry_tools
        ":layer_buildfarm_worker",
    ],
)

[
    oci_image_index(
        name = "buildfarm-%s" % image,
        images = [
            ":buildfarm-%s_linux_%s" % (image, arch)
            for arch in ARCH
        ],
        tags = ["container"],
    )
    for image in [
        "server",
        "worker",
    ]
]

######
# Helpers to write to the local Docker Desktop's registry
# Usage: `bazel run //:tarball_server_amd64 && docker run --rm buildfarm-server:amd64`
# ####
[
    [
        oci_tarball(
            name = "tarball_%s_%s" % (image, arch),
            image = ":buildfarm-%s_linux_%s" % (image, arch),
            repo_tags = ["buildfarm-%s:%s" % (image, arch)],
            tags = ["container"],
        ),
        # # Below targets push public docker images to bazelbuild dockerhub.
        oci_push(
            name = "public_push_buildfarm-%s" % image,
            image = ":buildfarm-%s" % image,
            remote_tags = [
                "$(release_version)",
            ],
            repository = "index.docker.io/bazelbuild/buildfarm-%s" % image,
            tags = ["container"],
        ),
    ]
    for arch in ARCH
    for image in [
        "server",
        "worker",
    ]
]
