load("@rules_java//java:java_test.bzl", "java_test")

COMMON_DEPS = [
    "//persistentworkers/src/main/java/persistent/common:persistent-common",
    "//persistentworkers/src/main/java/persistent/bazel:bazel-persistent-workers",
    "//persistentworkers/src/test/java/persistent/testutil:testutil",
    "//persistentworkers/src/main/protobuf:worker_protocol_java_proto",
    "@buildfarm_maven//:com_google_guava_guava",
    "@buildfarm_maven//:com_google_protobuf_protobuf_java",
    "@buildfarm_maven//:com_google_truth_truth",
    "@buildfarm_maven//:commons_io_commons_io",
    "@buildfarm_maven//:org_mockito_mockito_core",
]

java_test(
    name = "TestDepsTest",
    size = "small",
    srcs = ["processes/TestDepsTest.java"],
    resources = [
        "//persistentworkers/examples/src/main/java:adder-bin_deploy.jar",
    ],
    deps = COMMON_DEPS,
)

java_test(
    name = "ProtoWorkerRWTest",
    size = "small",
    srcs = ["processes/ProtoWorkerRWTest.java"],
    resources = [
        "//persistentworkers/examples/src/main/java:adder-bin_deploy.jar",
    ],
    deps = COMMON_DEPS,
)

java_test(
    name = "PersistentWorkerTest",
    size = "small",
    srcs = ["processes/PersistentWorkerTest.java"],
    resources = [
        "//persistentworkers/examples/src/main/java:adder-bin_deploy.jar",
    ],
    deps = COMMON_DEPS,
)
