load("@rules_java//java:java_library.bzl", "java_library")

java_library(
    name = "executionwrappers",
    srcs = glob(["*.java"]),
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/build/buildfarm/common",
        "//src/main/java/build/buildfarm/common/config",
        "@buildfarm_maven//:com_google_guava_guava",
        "@remoteapis//build/bazel/remote/execution/v2:remote_execution_java_proto",
    ],
)
