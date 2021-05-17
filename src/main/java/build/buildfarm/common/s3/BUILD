java_library(
    name = "S3Bucket",
    srcs = glob(["S3Bucket.java"]),
    visibility = ["//visibility:public"],
    deps = [
        "@maven//:com_amazonaws_aws_java_sdk_s3",
        "@maven//:com_amazonaws_aws_java_sdk_core",
        "@maven//:com_amazonaws_aws_java_sdk_secretsmanager",
        "@maven//:com_fasterxml_jackson_core_jackson_databind",
        #"@maven//:com_google_protobuf_protobuf_java_util",
        #"@remote_apis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
    ],
)
