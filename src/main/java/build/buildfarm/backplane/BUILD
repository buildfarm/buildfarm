java_library(
    name = "backplane",
    srcs = glob(["*.java"]),
    plugins = ["//src/main/java/build/buildfarm/common:lombok"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/build/buildfarm/common",
        "//src/main/java/build/buildfarm/instance",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@com_google_googleapis//google/longrunning:longrunning_java_proto",
        "@maven//:com_google_guava_guava",
        "@maven//:net_jcip_jcip_annotations",
        "@maven//:org_projectlombok_lombok",
        "@remoteapis//build/bazel/remote/execution/v2:remote_execution_java_proto",
    ],
)
