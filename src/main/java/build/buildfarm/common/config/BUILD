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
        "@buildfarm_maven//:com_github_oshi_oshi_core",
        "@buildfarm_maven//:com_github_pcj_google_options",
        "@buildfarm_maven//:com_google_auth_google_auth_library_oauth2_http",
        "@buildfarm_maven//:com_google_code_findbugs_jsr305",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java_util",
        "@buildfarm_maven//:io_grpc_grpc_api",
        "@buildfarm_maven//:me_dinowernli_java_grpc_prometheus",
        "@buildfarm_maven//:org_projectlombok_lombok",
        "@buildfarm_maven//:org_yaml_snakeyaml",
        "@buildfarm_maven//:redis_clients_jedis",
        "@googleapis//google/longrunning:longrunning_java_proto",
        "@googleapis//google/rpc:rpc_java_proto",
    ],
)
