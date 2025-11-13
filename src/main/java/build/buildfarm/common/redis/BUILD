load("@rules_java//java:java_library.bzl", "java_library")

java_library(
    name = "redis",
    srcs = glob(["*.java"]),
    plugins = ["//src/main/java/build/buildfarm/common:lombok"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/build/buildfarm/common",
        "//src/main/java/build/buildfarm/common/config",
        "//src/main/java/build/buildfarm/worker/resources",
        "//src/main/protobuf/build/buildfarm/v1test:buildfarm_java_proto",
        "@buildfarm_maven//:com_google_auth_google_auth_library_oauth2_http",
        "@buildfarm_maven//:com_google_code_findbugs_jsr305",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:io_grpc_grpc_api",
        "@buildfarm_maven//:org_apache_commons_commons_pool2",
        "@buildfarm_maven//:org_projectlombok_lombok",
        "@buildfarm_maven//:org_redisson_redisson",
        "@buildfarm_maven//:redis_clients_jedis",
        "@com_google_protobuf//java/core",
    ],
)
