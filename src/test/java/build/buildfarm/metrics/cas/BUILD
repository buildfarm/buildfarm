java_test(
    name = "tests",
    size = "small",
    srcs = glob(["*.java"]),
    test_class = "build.buildfarm.AllTests",
    deps = [
        "//src/main/java/build/buildfarm/backplane",
        "//src/main/java/build/buildfarm/metrics/cas",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "//src/test/java/build/buildfarm:test_runner",
        "@maven//:com_google_truth_truth",
        "@maven//:org_mockito_mockito_core",
    ],
)
