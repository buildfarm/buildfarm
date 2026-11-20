load("@rules_java//java:java_library.bzl", "java_library")

java_library(
    name = "testing",
    srcs = ["ControllableClientCall.java"],
    visibility = ["//src/test/java/build/buildfarm:__subpackages__"],
    deps = [
        "@buildfarm_maven//:com_google_code_findbugs_jsr305",
        "@buildfarm_maven//:io_grpc_grpc_api",
    ],
)
