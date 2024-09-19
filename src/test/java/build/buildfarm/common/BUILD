load("//:jvm_flags.bzl", "add_opens_sun_nio_fs", "ensure_accurate_metadata")

java_test(
    name = "tests",
    size = "small",
    srcs = glob(["*.java"]),
    jvm_flags = ensure_accurate_metadata() + add_opens_sun_nio_fs(),
    test_class = "build.buildfarm.AllTests",
    deps = [
        "//src/main/java/build/buildfarm/common",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "//src/test/java/build/buildfarm:test_runner",
        "@buildfarm_maven//:com_github_luben_zstd_jni",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:com_google_jimfs_jimfs",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java_util",
        "@buildfarm_maven//:com_google_truth_truth",
        "@buildfarm_maven//:io_grpc_grpc_api",
        "@buildfarm_maven//:io_grpc_grpc_core",
        "@buildfarm_maven//:io_grpc_grpc_stub",
        "@buildfarm_maven//:io_grpc_grpc_testing",
        "@buildfarm_maven//:org_mockito_mockito_core",
        "@buildfarm_maven//:redis_clients_jedis",
        "@googleapis//google/bytestream:bytestream_java_proto",
        "@googleapis//google/rpc:rpc_java_proto",
    ],
)
