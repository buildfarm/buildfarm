java_library(
    name = "redis",
    srcs = glob(["*.java"]),
    plugins = ["//src/main/java/build/buildfarm/common:lombok"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/build/buildfarm/common",
        "//src/main/java/build/buildfarm/common/config",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@buildfarm_maven//:com_google_code_findbugs_jsr305",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:io_grpc_grpc_api",
        "@buildfarm_maven//:org_projectlombok_lombok",
        "@buildfarm_maven//:org_redisson_redisson",
        "@buildfarm_maven//:redis_clients_jedis",
    ],
)
