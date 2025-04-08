load("@rules_java//java:java_binary.bzl", "java_binary")
load("//:jvm_flags.bzl", "add_opens_sun_nio_fs", "ensure_accurate_metadata")

package(
    default_visibility = ["//src:__subpackages__"],
)

java_binary(
    name = "buildfarm-server",
    data = [
        "//config:base_configs",
        "//config:base_properties",
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
    data = [
        "//config:base_configs",
        "//config:base_properties",
    ],
    jvm_flags = ensure_accurate_metadata() + add_opens_sun_nio_fs(),
    main_class = "build.buildfarm.worker.shard.Worker",
    visibility = ["//visibility:public"],
    runtime_deps = [
        "//src/main/java/build/buildfarm/worker/shard",
    ],
)
