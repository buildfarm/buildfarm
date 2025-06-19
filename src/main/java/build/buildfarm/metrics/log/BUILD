load("@rules_java//java:java_library.bzl", "java_library")

java_library(
    name = "log",
    srcs = glob(["*.java"]),
    plugins = ["//src/main/java/build/buildfarm/common:lombok"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/build/buildfarm/common/config",
        "//src/main/java/build/buildfarm/metrics",
        "//src/main/protobuf/build/buildfarm/v1test:buildfarm_java_proto",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java_util",
        "@buildfarm_maven//:org_projectlombok_lombok",
        "@googleapis//google/rpc:rpc_java_proto",
        "@remoteapis//build/bazel/remote/execution/v2:remote_execution_java_proto",
    ],
)
