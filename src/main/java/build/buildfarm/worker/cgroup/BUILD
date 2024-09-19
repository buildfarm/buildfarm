java_library(
    name = "cgroup",
    srcs = glob(["*.java"]),
    plugins = ["//src/main/java/build/buildfarm/common:lombok"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/build/buildfarm/worker",
        "@buildfarm_maven//:com_github_jnr_jnr_constants",
        "@buildfarm_maven//:com_github_jnr_jnr_posix",
        "@buildfarm_maven//:com_google_code_findbugs_jsr305",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:io_grpc_grpc_api",
        "@buildfarm_maven//:org_projectlombok_lombok",
    ],
)
