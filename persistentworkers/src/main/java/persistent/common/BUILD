load("@rules_java//java:java_library.bzl", "java_library")

java_library(
    name = "persistent-common",
    srcs = glob(["**/*.java"]),
    plugins = ["//src/main/java/build/buildfarm/common:lombok"],
    visibility = ["//visibility:public"],
    deps = [
        "//persistentworkers/src/main/java/persistent/common/util",
        "//persistentworkers/src/main/protobuf:worker_protocol_java_proto",
        "@buildfarm_maven//:com_github_pcj_google_options",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java_util",
        "@buildfarm_maven//:commons_io_commons_io",
        "@buildfarm_maven//:io_grpc_grpc_api",
        "@buildfarm_maven//:io_grpc_grpc_context",
        "@buildfarm_maven//:io_grpc_grpc_core",
        "@buildfarm_maven//:io_grpc_grpc_netty",
        "@buildfarm_maven//:io_grpc_grpc_protobuf",
        "@buildfarm_maven//:io_grpc_grpc_stub",
        "@buildfarm_maven//:org_apache_commons_commons_pool2",
        "@buildfarm_maven//:org_projectlombok_lombok",
    ],
)
