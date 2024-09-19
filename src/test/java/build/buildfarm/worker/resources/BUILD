java_test(
    name = "tests",
    size = "small",
    srcs = glob(["*.java"]),
    classpath_resources = [
        "//src/test/java/build/buildfarm:configs",
    ],
    test_class = "build.buildfarm.AllTests",
    deps = [
        "//src/main/java/build/buildfarm/common",
        "//src/main/java/build/buildfarm/common/config",
        "//src/main/java/build/buildfarm/worker/resources",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "//src/test/java/build/buildfarm:test_runner",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:com_google_jimfs_jimfs",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java",
        "@buildfarm_maven//:com_google_truth_truth",
        "@buildfarm_maven//:io_grpc_grpc_api",
        "@buildfarm_maven//:io_grpc_grpc_core",
        "@buildfarm_maven//:org_mockito_mockito_core",
    ],
)
