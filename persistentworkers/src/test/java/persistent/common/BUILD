load("@rules_java//java:java_test.bzl", "java_test")

java_test(
    name = "CoordinatorTest",
    size = "small",
    srcs = glob(["**/*.java"]),
    deps = [
        "//persistentworkers/src/main/java/persistent/common:persistent-common",
        "//persistentworkers/src/main/protobuf:worker_protocol_java_proto",
        "@buildfarm_maven//:com_google_truth_truth",
        "@buildfarm_maven//:commons_io_commons_io",
        "@buildfarm_maven//:org_mockito_mockito_core",
    ],
)
