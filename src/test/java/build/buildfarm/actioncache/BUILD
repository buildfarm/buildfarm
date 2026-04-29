load("@rules_java//java:java_test.bzl", "java_test")

java_test(
    name = "ShardActionCacheTest",
    size = "small",
    srcs = ["ShardActionCacheTest.java"],
    test_class = "build.buildfarm.AllTests",
    deps = [
        "//src/main/java/build/buildfarm/actioncache",
        "//src/main/java/build/buildfarm/backplane",
        "//src/main/java/build/buildfarm/common",
        "//src/main/protobuf/build/buildfarm/v1test:buildfarm_java_proto",
        "//src/test/java/build/buildfarm:test_runner",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:com_google_truth_truth",
        "@buildfarm_maven//:org_mockito_mockito_core",
        "@remoteapis//build/bazel/remote/execution/v2:remote_execution_java_proto",
    ],
)
