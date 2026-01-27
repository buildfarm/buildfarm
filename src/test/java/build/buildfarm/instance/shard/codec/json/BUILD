load("@rules_java//java:java_test.bzl", "java_test")

java_test(
    name = "tests",
    size = "small",
    srcs = glob(["*.java"]),
    test_class = "build.buildfarm.AllTests",
    runtime_deps = [
        "//src/test/java/build/buildfarm:test_runner",
    ],
    deps = [
        "//persistentworkers/src/main/protobuf:worker_protocol_java_proto",
        "//src/main/java/build/buildfarm/common/redis",
        "//src/main/java/build/buildfarm/instance/shard",
        "//src/main/protobuf/build/buildfarm/v1test:buildfarm_java_proto",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java",
        "@buildfarm_maven//:com_google_truth_truth",
    ],
)
