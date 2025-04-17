load("@rules_java//java:java_library.bzl", "java_library")

java_library(
    name = "config",
    srcs = glob([
        "*.java",
    ]),
    plugins = ["//src/main/java/build/buildfarm/common:lombok"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/build/buildfarm/common",
        "//src/main/java/build/buildfarm/common/base",
        "//src/main/protobuf/build/buildfarm/v1test:buildfarm_java_proto",
        "@googleapis//google/longrunning:longrunning_java_proto",
        "@googleapis//google/rpc:rpc_java_proto",
        "@maven//:com_github_oshi_oshi_core",
        "@maven//:com_github_pcj_google_options",
        "@maven//:com_google_auth_google_auth_library_oauth2_http",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:com_google_protobuf_protobuf_java_util",
        "@maven//:io_grpc_grpc_api",
        "@maven//:me_dinowernli_java_grpc_prometheus",
        "@maven//:org_projectlombok_lombok",
        "@maven//:org_redisson_redisson",
        "@maven//:org_yaml_snakeyaml",
        "@maven//:redis_clients_jedis",
    ],
)
