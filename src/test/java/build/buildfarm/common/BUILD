load("//:jvm_flags.bzl", "ensure_accurate_metadata")

java_test(
    name = "tests",
    size = "small",
    srcs = glob(["*.java"]),
    jvm_flags = ensure_accurate_metadata(),
    test_class = "build.buildfarm.AllTests",
    deps = [
        "//src/main/java/build/buildfarm/common",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "//src/test/java/build/buildfarm:test_runner",
        "//third_party/jedis",
        "@googleapis//:google_bytestream_bytestream_java_proto",
        "@googleapis//:google_rpc_error_details_java_proto",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_jimfs_jimfs",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:com_google_protobuf_protobuf_java_util",
        "@maven//:com_google_truth_truth",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_core",
        "@maven//:io_grpc_grpc_stub",
        "@maven//:io_grpc_grpc_testing",
        "@maven//:org_mockito_mockito_core",
    ],
)
