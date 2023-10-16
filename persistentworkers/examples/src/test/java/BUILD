java_test(
    name = "tests",
    size = "small",
    srcs = glob(["**/*.java"]),
    test_class = "adder.AdderTest",
    deps = [
        "//persistentworkers/examples/src/main/java:adder-lib",
        "//persistentworkers/src/main/java/persistent/bazel:bazel-persistent-workers",
        "//persistentworkers/src/main/java/persistent/common:persistent-common",
        "//persistentworkers/src/main/protobuf:worker_protocol_java_proto",
        "//persistentworkers/src/test/java/persistent/testutil",
        "@maven//:com_google_truth_truth",
        "@maven//:org_mockito_mockito_core",
    ],
)
