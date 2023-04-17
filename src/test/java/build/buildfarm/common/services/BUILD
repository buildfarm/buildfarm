load("//:jvm_flags.bzl", "ensure_accurate_metadata")

java_test(
    name = "tests",
    size = "small",
    srcs = glob(["*.java"]),
    jvm_flags = ensure_accurate_metadata(),
    test_class = "build.buildfarm.AllTests",
    deps = [
        "//src/main/java/build/buildfarm/common",
        "//src/main/java/build/buildfarm/common/resources",
        "//src/main/java/build/buildfarm/common/resources:resource_java_proto",
        "//src/main/java/build/buildfarm/common/services",
        "//src/main/java/build/buildfarm/instance",
        "//src/main/java/build/buildfarm/instance/stub",
        "//src/test/java/build/buildfarm:test_runner",
        "@googleapis//:google_bytestream_bytestream_java_grpc",
        "@googleapis//:google_bytestream_bytestream_java_proto",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:com_google_truth_truth",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_context",
        "@maven//:io_grpc_grpc_core",
        "@maven//:io_grpc_grpc_stub",
        "@maven//:org_mockito_mockito_core",
        "@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
    ],
)
