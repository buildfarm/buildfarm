load("@rules_java//java:java_test.bzl", "java_test")

java_test(
    name = "tests",
    size = "small",
    srcs = glob(["*.java"]),
    data = ["//examples:example_configs"],
    test_class = "build.buildfarm.AllTests",
    deps = [
        "//src/main/java/build/buildfarm/common",
        "//src/main/java/build/buildfarm/common/config",
        "//src/main/java/build/buildfarm/instance",
        "//src/main/java/build/buildfarm/server/services",
        "//src/main/protobuf/build/buildfarm/v1test:buildfarm_java_proto",
        "//src/test/java/build/buildfarm:test_runner",
        "//third_party/remote-apis:build_bazel_remote_asset_v1_remote_asset_java_grpc",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:com_google_truth_truth",
        "@maven//:io_grpc_grpc_stub",
        "@maven//:org_mockito_mockito_core",
        "@remoteapis//build/bazel/remote/asset/v1:remote_asset_java_proto",
    ],
)
