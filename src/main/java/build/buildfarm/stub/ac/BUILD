load("@rules_java//java:java_library.bzl", "java_library")

java_library(
    name = "ac",
    srcs = glob(["*.java"]),
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/build/buildfarm/ac",
        "//src/main/java/build/buildfarm/common",
        "//src/main/protobuf/build/buildfarm/v1test:buildfarm_java_proto",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:io_grpc_grpc_api",
        "@remoteapis//build/bazel/remote/execution/v2:remote_execution_java_grpc",
        "@remoteapis//build/bazel/remote/execution/v2:remote_execution_java_proto",
    ],
)
