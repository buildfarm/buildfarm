load("@rules_java//java:java_test.bzl", "java_test")
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
        "//src/main/protobuf/build/buildfarm/v1test:buildfarm_java_proto",
        "//src/test/java/build/buildfarm:test_runner",
        "//third_party/remote-apis:build_bazel_remote_execution_v2_remote_execution_java_grpc",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java",
        "@buildfarm_maven//:com_google_truth_truth",
        "@buildfarm_maven//:io_grpc_grpc_api",
        "@buildfarm_maven//:io_grpc_grpc_context",
        "@buildfarm_maven//:io_grpc_grpc_core",
        "@buildfarm_maven//:io_grpc_grpc_inprocess",
        "@buildfarm_maven//:io_grpc_grpc_stub",
        "@buildfarm_maven//:org_mockito_mockito_core",
        "@googleapis//google/bytestream:bytestream_java_grpc",
        "@googleapis//google/bytestream:bytestream_java_proto",
        "@remoteapis//build/bazel/remote/execution/v2:remote_execution_java_proto",
    ],
)
