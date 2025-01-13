load("@rules_java//java:java_library.bzl", "java_library")

java_library(
    name = "cgroup",
    srcs = glob(["*.java"]),
    plugins = ["//src/main/java/build/buildfarm/common:lombok"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/build/buildfarm/worker",
        "@maven//:com_github_jnr_jnr_constants",
        "@maven//:com_github_jnr_jnr_posix",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_guava_guava",
        "@maven//:io_grpc_grpc_api",
        "@maven//:org_projectlombok_lombok",
    ],
)
