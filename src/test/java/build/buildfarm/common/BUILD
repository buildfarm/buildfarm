load("//:jvm_flags.bzl", "add_opens_sun_nio_fs", "ensure_accurate_metadata")

java_test(
    name = "tests",
    size = "small",
    srcs = glob(["*.java"]),
    jvm_flags = ensure_accurate_metadata() + add_opens_sun_nio_fs(),
    test_class = "build.buildfarm.AllTests",
    deps = [
        "//src/main/java/build/buildfarm/common",
        "//src/main/protobuf/build/buildfarm/v1test:buildfarm_java_proto",
        "//src/test/java/build/buildfarm:test_runner",
        "@googleapis//google/bytestream:bytestream_java_proto",
        "@googleapis//google/rpc:rpc_java_proto",
        "@maven//:com_github_luben_zstd_jni",
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
        "@maven//:redis_clients_jedis",
    ],
)
