java_library(
    name = "metrics",
    srcs = glob(["*.java"]),
    plugins = ["//src/main/java/build/buildfarm/common:lombok"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/build/buildfarm/common",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//google/rpc:rpc_java_proto",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:com_google_protobuf_protobuf_java_util",
        "@maven//:io_prometheus_simpleclient",
        "@maven//:org_projectlombok_lombok",
        "@remoteapis//build/bazel/remote/execution/v2:remote_execution_java_proto",
    ],
)
