load("//:jvm_flags.bzl", "add_opens_sun_nio_fs", "ensure_accurate_metadata")

package(
    default_visibility = ["//src:__subpackages__"],
)

filegroup(
    name = "configs",
    srcs = ["logging.properties"],
    visibility = ["//visibility:public"],
)

java_binary(
    name = "buildfarm-server",
    classpath_resources = [
        ":configs",
    ],
    jvm_flags = ensure_accurate_metadata() + add_opens_sun_nio_fs(),
    main_class = "build.buildfarm.server.BuildFarmServer",
    visibility = ["//visibility:public"],
    runtime_deps = [
        "//src/main/java/build/buildfarm/server",
        "@maven//:org_slf4j_slf4j_simple",
    ],
)

java_binary(
    name = "buildfarm-shard-worker",
    classpath_resources = [
        ":configs",
    ],
    jvm_flags = ensure_accurate_metadata() + add_opens_sun_nio_fs(),
    main_class = "build.buildfarm.worker.shard.Worker",
    visibility = ["//visibility:public"],
    runtime_deps = [
        "//src/main/java/build/buildfarm/worker/shard",
        "@maven//:org_slf4j_slf4j_simple",
    ],
)
