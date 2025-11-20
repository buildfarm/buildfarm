load("@rules_img//img:image.bzl", "image_manifest")
load("@rules_img//img:layer.bzl", "layer_from_tar")
load("@rules_pkg//:pkg.bzl", "pkg_tar")
load("@rules_pkg//pkg:mappings.bzl", "pkg_attributes", "pkg_files")
load("//container:defs.bzl", "multiarch_oci_image")

package(default_visibility = ["//visibility:public"])

# == Docker Image Creation ==
# When deploying buildfarm, you may want to include additional dependencies within your deployment.
# These dependencies can enable features related to the observability and runtime of the system.
# For example, "debgging tools", "introspection tools", and "exeution wrappers" are examples of dependencies
# that many need included within deployed containers.  This BUILD file creates docker images that bundle
# additional dependencies alongside the buildfarm agents.

DEFAULT_IMAGE_LABELS = {
    "org.opencontainers.image.source": "https://github.com/buildfarm/buildfarm",
}

DEFAULT_PACKAGE_DIR = "app/build_buildfarm"

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
pkg_tar(
    name = "execution_wrappers",
    srcs = select({
        "@platforms//cpu:aarch64": [],
        "//conditions:default": [
            ":exec-wrapper-helpers",
        ],
    }) + [
        ":exec-wrapper-files",
    ],
    package_dir = DEFAULT_PACKAGE_DIR,
    tags = ["container"],
)

pkg_tar(
    name = "telemetry_tools",
    srcs = [
        ":opentelemetry-javaagent",
    ],
    package_dir = DEFAULT_PACKAGE_DIR,
    tags = ["container"],
)

pkg_files(
    name = "tini.binary",
    srcs = ["@tini//file"],
    attributes = pkg_attributes(
        mode = "0555",
    ),
    renames = {
        "@tini//file": "tini",
    },
    tags = ["container"],
)

pkg_files(
    name = "tini.binary-arm64v8",
    srcs = ["@tini_arm64v8//file"],
    attributes = pkg_attributes(
        mode = "0555",
    ),
    renames = {
        "@tini_arm64v8//file": "tini",
    },
    tags = ["container"],
)

pkg_files(
    name = "opentelemetry-javaagent",
    srcs = ["@opentelemetry//jar"],
    attributes = pkg_attributes(
        mode = "0444",
    ),
    renames = {
        "@opentelemetry//jar": "opentelemetry-javaagent.jar",
    },
    tags = ["container"],
)

pkg_files(
    name = "exec-wrapper-files",
    srcs = select({
        "@platforms//cpu:aarch64": [],
        "//conditions:default": [
            "@skip_sleep",
            # The delay wrapper is only intended to be used with the "skip_sleep" wrapper.
            "//:delay.sh",
        ],
    }) + [
        # keep sorted
        "//:as-nobody",
        "//:cgexec-wrapper",
        "//:macos-wrapper.sh",
        "@bazel//src/main/tools:linux-sandbox",
        "@bazel//src/main/tools:process-wrapper",
    ],
    attributes = pkg_attributes(
        mode = "0555",
    ),
    tags = ["container"],
)

pkg_files(
    name = "exec-wrapper-helpers",
    srcs = ["@skip_sleep//:skip_sleep_preload"],
    attributes = pkg_attributes(
        mode = "0444",
    ),
    prefix = DEFAULT_PACKAGE_DIR,
    renames = {
        "@skip_sleep//:skip_sleep_preload": "skip_sleep_preload.so",
    },
    tags = ["container"],
)

pkg_tar(
    name = "layer_tini",
    srcs = select({
        "@platforms//cpu:aarch64": [
            ":tini.binary-arm64v8",
        ],
        "//conditions:default": [
            ":tini.binary",
        ],
    }),
    tags = ["container"],
)

pkg_tar(
    name = "layer_buildfarm_server",
    srcs = ["//src/main/java/build/buildfarm:buildfarm-server_deploy.jar"],
    package_dir = DEFAULT_PACKAGE_DIR,
    tags = ["container"],
)

pkg_tar(
    name = "layer_buildfarm_worker",
    srcs = ["//src/main/java/build/buildfarm:buildfarm-shard-worker_deploy.jar"],
    package_dir = DEFAULT_PACKAGE_DIR,
    tags = ["container"],
)

pkg_tar(
    name = "layer_minimal_config",
    srcs = ["@build_buildfarm//examples:example_configs"],
    package_dir = DEFAULT_PACKAGE_DIR,
    tags = ["container"],
)

pkg_tar(
    name = "layer_logging_config",
    srcs = ["@build_buildfarm//examples:example_properties"],
    package_dir = DEFAULT_PACKAGE_DIR + "/src/main/java/build/buildfarm",
    tags = ["container"],
)

# Note: We can't use server_jvm_flags() and worker_jvm_flags() directly in BUILD
# because they return lists with select() which can't be joined at load time.
# We need to inline the computation or use a genrule/rule to compute env vars.

# For now, let's use a simplified version without selects for the base case
# and handle telemetry flags separately if needed

_BASE_JVM_FLAGS = [
    "-XX:+UseContainerSupport",
    "-XX:MaxRAMPercentage=80.0",
    "-XX:+UseStringDeduplication",
    "-XX:+UseCompressedOops",
    "-XX:+HeapDumpOnOutOfMemoryError",
    "-Dlogging.config=file:/app/build_buildfarm/src/main/java/build/buildfarm/logging.properties",
]

# Environment variables for server
SERVER_ENV = {
    "CONFIG_PATH": "/" + DEFAULT_PACKAGE_DIR + "/config.minimal.yml",
    "JAVA_TOOL_OPTIONS": " ".join(_BASE_JVM_FLAGS),
}

# Environment variables for worker
WORKER_ENV = {
    "CONFIG_PATH": "/" + DEFAULT_PACKAGE_DIR + "/config.minimal.yml",
    "JAVA_TOOL_OPTIONS": " ".join(_BASE_JVM_FLAGS),
}

layer_from_tar(
    name = "_layer_logging_config_server",
    src = ":layer_logging_config",
    tags = ["container"],
)

layer_from_tar(
    name = "_layer_minimal_config_server",
    src = ":layer_minimal_config",
    tags = ["container"],
)

layer_from_tar(
    name = "_layer_telemetry_tools_server",
    src = ":telemetry_tools",
    tags = ["container"],
)

layer_from_tar(
    name = "_layer_buildfarm_server_jar",
    src = ":layer_buildfarm_server",
    tags = ["container"],
)

image_manifest(
    name = "_buildfarm-server.image",
    base = "@amazon_corretto_java_image_base",
    entrypoint = [
        "java",
        "-jar",
        "/" + DEFAULT_PACKAGE_DIR + "/buildfarm-server_deploy.jar",
    ],
    env = SERVER_ENV,
    labels = DEFAULT_IMAGE_LABELS,
    layers = [
        # do not sort
        ":_layer_logging_config_server",
        ":_layer_minimal_config_server",
        ":_layer_telemetry_tools_server",
        ":_layer_buildfarm_server_jar",
    ],
    tags = ["container"],
    visibility = ["//container:__subpackages__"],
)

layer_from_tar(
    name = "_layer_tini",
    src = ":layer_tini",
    tags = ["container"],
)

layer_from_tar(
    name = "_layer_logging_config_worker",
    src = ":layer_logging_config",
    tags = ["container"],
)

layer_from_tar(
    name = "_layer_minimal_config_worker",
    src = ":layer_minimal_config",
    tags = ["container"],
)

layer_from_tar(
    name = "_layer_execution_wrappers",
    src = ":execution_wrappers",
    tags = ["container"],
)

layer_from_tar(
    name = "_layer_telemetry_tools_worker",
    src = ":telemetry_tools",
    tags = ["container"],
)

layer_from_tar(
    name = "_layer_buildfarm_worker_jar",
    src = ":layer_buildfarm_worker",
    tags = ["container"],
)

image_manifest(
    name = "_buildfarm-worker.image",
    base = "@ubuntu_noble",
    entrypoint = [
        # do not sort
        "/tini",
        "--",
        "java",
        "-jar",
        "/" + DEFAULT_PACKAGE_DIR + "/buildfarm-shard-worker_deploy.jar",
    ],
    env = WORKER_ENV,
    labels = DEFAULT_IMAGE_LABELS,
    layers = [
        # do not sort
        ":_layer_tini",
        ":_layer_logging_config_worker",
        ":_layer_minimal_config_worker",
        ":_layer_execution_wrappers",
        ":_layer_telemetry_tools_worker",
        ":_layer_buildfarm_worker_jar",
    ],
    tags = ["container"],
    visibility = ["//container:__subpackages__"],
)

multiarch_oci_image(
    name = "buildfarm-server",
    image = ":_buildfarm-server.image",
)

multiarch_oci_image(
    name = "buildfarm-worker",
    image = ":_buildfarm-worker.image",
)
