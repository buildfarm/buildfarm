load("@rules_java//java:java_test.bzl", "java_test")

java_test(
    name = "tests",
    size = "small",
    srcs = glob(["*.java"]),
    test_class = "build.buildfarm.AllTests",
    deps = [
        "//src/main/java/build/buildfarm/common/base",
        "//src/main/java/build/buildfarm/worker/cgroup",
        "//src/test/java/build/buildfarm:test_runner",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_truth_truth",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_projectlombok_lombok",
    ],
)
