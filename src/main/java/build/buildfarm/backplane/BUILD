java_library(
    name = "backplane",
    srcs = glob(["*.java"]),
    plugins = ["//src/main/java/build/buildfarm/common:lombok"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/build/buildfarm/common",
        "//src/main/java/build/buildfarm/instance",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:net_jcip_jcip_annotations",
        "@buildfarm_maven//:org_projectlombok_lombok",
        "@googleapis//google/longrunning:longrunning_java_proto",
        "@remoteapis//build/bazel/remote/execution/v2:remote_execution_java_proto",
    ],
)
