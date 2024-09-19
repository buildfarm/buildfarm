java_library(
    name = "metrics",
    srcs = glob(["*.java"]),
    plugins = ["//src/main/java/build/buildfarm/common:lombok"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/build/buildfarm/common",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java_util",
        "@buildfarm_maven//:io_prometheus_simpleclient",
        "@buildfarm_maven//:org_projectlombok_lombok",
        "@googleapis//google/rpc:rpc_java_proto",
        "@remoteapis//build/bazel/remote/execution/v2:remote_execution_java_proto",
    ],
)
