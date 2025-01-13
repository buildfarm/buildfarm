load("@rules_java//java:java_test.bzl", "java_test")

java_test(
    name = "tests",
    size = "small",
    srcs = ["S3BucketMockTest.java"],
    test_class = "build.buildfarm.AllTests",
    deps = [
        "//src/main/java/build/buildfarm/common/s3:S3Bucket",
        "//src/test/java/build/buildfarm:test_runner",
        "@maven//:com_amazonaws_aws_java_sdk_s3",
        "@maven//:com_google_truth_truth",
        "@maven//:io_grpc_grpc_api",
        "@maven//:org_mockito_mockito_core",
    ],
)
