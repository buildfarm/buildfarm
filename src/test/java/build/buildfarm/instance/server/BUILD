load("@rules_java//java:java_test.bzl", "java_test")

java_test(
    name = "tests",
    size = "small",
    srcs = glob(["*.java"]),
    plugins = ["//src/main/java/build/buildfarm/common:lombok"],
    test_class = "build.buildfarm.AllTests",
    deps = [
        "//src/main/java/build/buildfarm/actioncache",
        "//src/main/java/build/buildfarm/cas",
        "//src/main/java/build/buildfarm/common",
        "//src/main/java/build/buildfarm/instance",
        "//src/main/java/build/buildfarm/instance/server",
        "//src/main/protobuf/build/buildfarm/v1test:buildfarm_java_proto",
        "//src/test/java/build/buildfarm:test_runner",
        "@googleapis//google/longrunning:longrunning_java_proto",
        "@googleapis//google/rpc:rpc_java_proto",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:com_google_truth_truth",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_stub",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_projectlombok_lombok",
        "@remoteapis//build/bazel/remote/execution/v2:remote_execution_java_proto",
    ],
)
