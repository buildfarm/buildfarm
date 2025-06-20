load("@rules_java//java:java_test.bzl", "java_test")

java_test(
    name = "tests",
    srcs = glob(["*Test.java"]),
    test_class = "build.buildfarm.AllTests",
    deps = [
        "//src/main/java/build/buildfarm/common/config",
        "//src/test/java/build/buildfarm:test_runner",
        "@buildfarm_maven//:com_google_truth_truth",
        "@buildfarm_maven//:io_grpc_grpc_api",
        "@buildfarm_maven//:io_grpc_grpc_testing",
        "@buildfarm_maven//:me_dinowernli_java_grpc_prometheus",
        "@buildfarm_maven//:org_mockito_mockito_core",
    ],
)
