load("@rules_java//java:java_library.bzl", "java_library")

java_library(
    name = "testutil",
    testonly = True,
    srcs = glob(["**/*.java"]),
    visibility = ["//visibility:public"],
    deps = [
        "//persistentworkers/src/main/java/persistent/bazel:bazel-persistent-workers",
        "//persistentworkers/src/main/java/persistent/common:persistent-common",
        "//persistentworkers/src/main/protobuf:worker_protocol_java_proto",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java",
        "@buildfarm_maven//:com_google_truth_truth",
        "@buildfarm_maven//:commons_io_commons_io",
        "@buildfarm_maven//:org_mockito_mockito_core",
    ],
)
