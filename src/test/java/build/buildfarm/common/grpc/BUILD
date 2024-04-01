java_library(
    name = "grpc",
    srcs = ["ByteStreamServiceWriter.java"],
    visibility = ["//src/test/java/build/buildfarm:__subpackages__"],
    deps = [
        "//src/main/java/build/buildfarm/common/grpc",
        "@com_google_googleapis//google/bytestream:bytestream_java_grpc",
        "@com_google_googleapis//google/bytestream:bytestream_java_proto",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_core",
        "@maven//:io_grpc_grpc_stub",
    ],
)

java_test(
    name = "tests",
    srcs = glob(["*Test.java"]),
    test_class = "build.buildfarm.AllTests",
    deps = [
        "//src/main/java/build/buildfarm/common/grpc",
        "//src/test/java/build/buildfarm:test_runner",
        "@com_google_googleapis//google/bytestream:bytestream_java_grpc",
        "@com_google_googleapis//google/bytestream:bytestream_java_proto",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:com_google_truth_truth",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_core",
        "@maven//:io_grpc_grpc_inprocess",
        "@maven//:io_grpc_grpc_stub",
        "@maven//:io_grpc_grpc_testing",
        "@maven//:io_grpc_grpc_util",
        "@maven//:org_mockito_mockito_core",
    ],
)
