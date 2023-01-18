java_test(
    name = "CoordinatorTest",
    size = "small",
    srcs = glob(["**/*.java"]),
    deps = [
        "//persistentworkers/src/main/java/persistent/common:persistent-common",
        "//persistentworkers/src/test/java/persistent/testutil",
        "@bazel_tools//src/main/protobuf:worker_protocol_java_proto",
        "@maven//:com_google_truth_truth",
        "@maven//:commons_io_commons_io",
        "@maven//:org_mockito_mockito_core",
    ],
)
