load("@rules_java//java:java_library.bzl", "java_library")

java_library(
    name = "grpc",
    srcs = glob(["*.java"]),
    plugins = ["//src/main/java/build/buildfarm/common:lombok"],
    visibility = ["//visibility:public"],
    runtime_deps = [
        "@buildfarm_maven//:org_slf4j_slf4j_simple",
    ],
    deps = [
        "//src/main/java/build/buildfarm/common",
        "@buildfarm_maven//:com_google_code_findbugs_jsr305",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java",
        "@buildfarm_maven//:io_grpc_grpc_api",
        "@buildfarm_maven//:io_grpc_grpc_context",
        "@buildfarm_maven//:io_grpc_grpc_core",
        "@buildfarm_maven//:io_grpc_grpc_netty",
        "@buildfarm_maven//:io_grpc_grpc_protobuf",
        "@buildfarm_maven//:io_grpc_grpc_stub",
        "@buildfarm_maven//:org_projectlombok_lombok",
        "@googleapis//google/bytestream:bytestream_java_grpc",
        "@googleapis//google/bytestream:bytestream_java_proto",
        "@remoteapis//build/bazel/remote/execution/v2:remote_execution_java_proto",
    ],
)
