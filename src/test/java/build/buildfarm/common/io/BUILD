load("@rules_java//java:java_test.bzl", "java_test")

java_test(
    name = "tests",
    srcs = glob(["*.java"]),
    test_class = "build.buildfarm.AllTests",
    deps = [
        "//src/main/java/build/buildfarm/common",
        "//src/main/java/build/buildfarm/common/base",
        "//src/main/java/build/buildfarm/common/grpc",
        "//src/test/java/build/buildfarm:test_runner",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:com_google_jimfs_jimfs",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java",
        "@buildfarm_maven//:com_google_truth_truth",
        "@buildfarm_maven//:commons_io_commons_io",
        "@buildfarm_maven//:io_grpc_grpc_api",
        "@buildfarm_maven//:io_grpc_grpc_core",
        "@buildfarm_maven//:io_grpc_grpc_stub",
        "@buildfarm_maven//:io_grpc_grpc_testing",
        "@buildfarm_maven//:org_apache_commons_commons_compress",
        "@buildfarm_maven//:org_mockito_mockito_core",
        "@googleapis//google/bytestream:bytestream_java_grpc",
        "@googleapis//google/bytestream:bytestream_java_proto",
    ],
)
