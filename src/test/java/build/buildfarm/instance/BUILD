java_test(
    name = "tests",
    size = "small",
    srcs = glob(["*.java"]),
    test_class = "build.buildfarm.AllTests",
    deps = [
        "//src/main/java/build/buildfarm/common",
        "//src/main/java/build/buildfarm/instance",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "//src/test/java/build/buildfarm:test_runner",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java",
        "@buildfarm_maven//:com_google_truth_truth",
        "@buildfarm_maven//:io_grpc_grpc_protobuf",
        "@buildfarm_maven//:io_grpc_grpc_stub",
        "@googleapis//google/longrunning:longrunning_java_proto",
        "@remoteapis//build/bazel/remote/execution/v2:remote_execution_java_proto",
    ],
)
