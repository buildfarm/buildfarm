load("@rules_java//java:java_library.bzl", "java_library")

java_library(
    name = "S3Bucket",
    srcs = ["S3Bucket.java"],
    plugins = ["//src/main/java/build/buildfarm/common:lombok"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/protobuf/build/buildfarm/v1test:buildfarm_java_proto",
        "@buildfarm_maven//:com_amazonaws_aws_java_sdk_core",
        "@buildfarm_maven//:com_amazonaws_aws_java_sdk_s3",
        "@buildfarm_maven//:com_amazonaws_aws_java_sdk_secretsmanager",
        "@buildfarm_maven//:com_fasterxml_jackson_core_jackson_databind",
        "@buildfarm_maven//:org_projectlombok_lombok",
    ],
)
