java_library(
    name = "backplane",
    srcs = glob(["*.java"]),
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/build/buildfarm/common",
        "//src/main/java/build/buildfarm/instance",
        "//src/main/java/build/buildfarm/operations",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@bazel_remote_apis//build/bazel/remote/execution/v2:remote_execution_java_grpc",
        "@com_google_googleapis//google/longrunning:longrunning_java_proto",
        "@maven//:com_google_guava_guava",
        "@maven//:net_jcip_jcip_annotations",
    ],
)
