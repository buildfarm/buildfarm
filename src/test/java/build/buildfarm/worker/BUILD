config_setting(
    name = "fuse",
    values = {"define": "fuse=true"},
)

java_test(
    name = "tests",
    size = "small",
    srcs = glob(
        ["*.java"],
        exclude = ["FuseCASTest.java"],
    ) + select({
        ":fuse": ["FuseCASTest.java"],
        "//conditions:default": [],
    }),
    data = ["//examples:example_configs"],
    plugins = ["//src/main/java/build/buildfarm/common:lombok"],
    test_class = "build.buildfarm.AllTests",
    deps = [
        "//src/main/java/build/buildfarm/cas",
        "//src/main/java/build/buildfarm/common",
        "//src/main/java/build/buildfarm/common/config",
        "//src/main/java/build/buildfarm/instance",
        "//src/main/java/build/buildfarm/worker",
        "//src/main/java/build/buildfarm/worker/resources",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "//src/test/java/build/buildfarm:test_runner",
        "@buildfarm_maven//:com_github_jnr_jnr_constants",
        "@buildfarm_maven//:com_github_jnr_jnr_ffi",
        "@buildfarm_maven//:com_github_serceman_jnr_fuse",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:com_google_jimfs_jimfs",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java",
        "@buildfarm_maven//:com_google_truth_truth",
        "@buildfarm_maven//:io_grpc_grpc_api",
        "@buildfarm_maven//:io_grpc_grpc_context",
        "@buildfarm_maven//:io_grpc_grpc_core",
        "@buildfarm_maven//:io_grpc_grpc_protobuf",
        "@buildfarm_maven//:org_mockito_mockito_core",
        "@buildfarm_maven//:org_projectlombok_lombok",
        "@googleapis//google/rpc:rpc_java_proto",
        "@remoteapis//build/bazel/remote/execution/v2:remote_execution_java_proto",
    ],
)
