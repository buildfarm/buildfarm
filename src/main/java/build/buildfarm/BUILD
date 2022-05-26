load("@io_bazel_rules_docker//container:container.bzl", "container_image")
load("//:defs.bzl", "ensure_accurate_metadata")

package(
    default_visibility = ["//src:__subpackages__"],
)

filegroup(
    name = "configs",
    srcs = ["logging.properties"],
    visibility = ["//visibility:public"],
)

genrule(
    name = "tini.binary",
    srcs = ["@tini//file"],
    outs = ["tini"],
    cmd = "cp $< $@ && chmod +x $@",
)

java_binary(
    name = "buildfarm-server",
    classpath_resources = [
        ":configs",
    ],
    jvm_flags = ensure_accurate_metadata(),
    main_class = "build.buildfarm.server.BuildFarmServer",
    visibility = ["//visibility:public"],
    runtime_deps = [
        "//src/main/java/build/buildfarm/server",
    ],
)

container_image(
    name = "server.container",
    base = "@java_image_base//image",
    cmd = [
        "buildfarm-server_deploy.jar",
        "/config/server.config",
        "--port",
        "8980",
    ],
    # leverage the implicit target of the buildfarm-server to get a fat jar.
    # this is simply a workaround for the fact that we have so many dependencies,
    # so we'd want some wrappy script. This seemed more straightforward.
    # https://docs.bazel.build/versions/master/be/java.html#java_binary_implicit_outputs
    files = [
        ":buildfarm-server_deploy.jar",
    ],
    tags = ["container"],
    visibility = ["//visibility:public"],
)

java_binary(
    name = "buildfarm-shard-worker",
    classpath_resources = [
        ":configs",
    ],
    jvm_flags = ensure_accurate_metadata(),
    main_class = "build.buildfarm.worker.shard.Worker",
    visibility = ["//visibility:public"],
    runtime_deps = [
        "//src/main/java/build/buildfarm/worker/shard",
    ],
)

container_image(
    name = "buildfarm-shard-worker.container",
    base = "@java_image_base//image",
    # leverage the implicit target of the buildfarm-shard-worker to get a fat jar.
    # this is simply a workaround for the fact that we have so many dependencies,
    # so we'd want some wrappy script. This seemed more straightforward.
    # https://docs.bazel.build/versions/master/be/java.html#java_binary_implicit_outputs
    files = [
        ":buildfarm-shard-worker_deploy.jar",
    ],
    tags = ["container"],
    visibility = ["//visibility:public"],
)

java_binary(
    name = "buildfarm-memory-worker",
    jvm_flags = ensure_accurate_metadata(),
    main_class = "build.buildfarm.worker.memory.Worker",
    visibility = ["//visibility:public"],
    runtime_deps = [
        "//src/main/java/build/buildfarm/worker/memory",
    ],
)

container_image(
    name = "buildfarm-memory-worker.container",
    base = "@java_image_base//image",
    cmd = [
        "buildfarm-memory-worker_deploy.jar",
        "/config/worker.config",
    ],
    # leverage the implicit target of the buildfarm-server to get a fat jar.
    # this is simply a workaround for the fact that we have so many dependencies,
    # so we'd want some wrappy script. This seemed more straightforward.
    # https://docs.bazel.build/versions/master/be/java.html#java_binary_implicit_outputs
    files = [
        ":buildfarm-memory-worker_deploy.jar",
    ],
    tags = ["container"],
    visibility = ["//visibility:public"],
)
