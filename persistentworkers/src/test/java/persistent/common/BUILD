load("@rules_java//java:java_test.bzl", "java_test")

java_test(
    name = "CoordinatorTest",
    size = "small",
    srcs = glob(["**/*.java"]),
    deps = [
        "//persistentworkers/src/main/java/persistent/common:persistent-common",
        "//persistentworkers/src/main/protobuf:worker_protocol_java_proto",
        "//persistentworkers/src/test/java/persistent/testutil",
        "@maven//:com_google_truth_truth",
        "@maven//:commons_io_commons_io",
        "@maven//:org_mockito_mockito_core",
    ],
)
