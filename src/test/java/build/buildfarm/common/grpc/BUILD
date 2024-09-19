java_library(
    name = "grpc",
    srcs = ["ByteStreamServiceWriter.java"],
    visibility = ["//src/test/java/build/buildfarm:__subpackages__"],
    deps = [
        "//src/main/java/build/buildfarm/common/grpc",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java",
        "@buildfarm_maven//:io_grpc_grpc_api",
        "@buildfarm_maven//:io_grpc_grpc_core",
        "@buildfarm_maven//:io_grpc_grpc_stub",
        "@googleapis//google/bytestream:bytestream_java_grpc",
        "@googleapis//google/bytestream:bytestream_java_proto",
    ],
)

java_test(
    name = "tests",
    srcs = glob(["*Test.java"]),
    test_class = "build.buildfarm.AllTests",
    deps = [
        "//src/main/java/build/buildfarm/common/grpc",
        "//src/test/java/build/buildfarm:test_runner",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java",
        "@buildfarm_maven//:com_google_truth_truth",
        "@buildfarm_maven//:io_grpc_grpc_api",
        "@buildfarm_maven//:io_grpc_grpc_core",
        "@buildfarm_maven//:io_grpc_grpc_inprocess",
        "@buildfarm_maven//:io_grpc_grpc_stub",
        "@buildfarm_maven//:io_grpc_grpc_testing",
        "@buildfarm_maven//:io_grpc_grpc_util",
        "@buildfarm_maven//:org_mockito_mockito_core",
        "@googleapis//google/bytestream:bytestream_java_grpc",
        "@googleapis//google/bytestream:bytestream_java_proto",
    ],
)
