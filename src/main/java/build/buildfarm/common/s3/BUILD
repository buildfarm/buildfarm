java_library(
    name = "S3Bucket",
    srcs = ["S3Bucket.java"],
    plugins = ["//src/main/java/build/buildfarm/common:lombok"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/protobuf/build/buildfarm/v1test:buildfarm_java_proto",
        "@maven//:com_amazonaws_aws_java_sdk_core",
        "@maven//:com_amazonaws_aws_java_sdk_s3",
        "@maven//:com_amazonaws_aws_java_sdk_secretsmanager",
        "@maven//:com_fasterxml_jackson_core_jackson_databind",
        "@maven//:org_projectlombok_lombok",
    ],
)
