load("@rules_java//java:java_test.bzl", "java_test")

java_test(
    name = "tests",
    size = "small",
    srcs = glob(["**/*.java"]),
    test_class = "persistent.examples.adder.AdderTest",
    deps = [
        "//src/main/java/persistent/bazel:bazel-persistent-workers",
        "//src/main/java/persistent/common:persistent-common",
        "//src/main/java/persistent/examples:adder-lib",
        "//src/main/protobuf/persistent:worker_protocol_java_proto",
        "//src/test/java/persistent/testutil",
        "@buildfarm_maven//:com_google_truth_truth",
        "@buildfarm_maven//:org_mockito_mockito_core",
    ],
)
