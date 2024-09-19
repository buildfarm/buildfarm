java_test(
    name = "tests",
    size = "small",
    srcs = glob(["*.java"]),
    test_class = "build.buildfarm.AllTests",
    deps = [
        "//src/main/java/build/buildfarm/worker/cgroup",
        "//src/test/java/build/buildfarm:test_runner",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:com_google_truth_truth",
        "@buildfarm_maven//:org_mockito_mockito_core",
        "@buildfarm_maven//:org_projectlombok_lombok",
    ],
)
