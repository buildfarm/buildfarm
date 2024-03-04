java_library(
    name = "operations",
    srcs = glob(
        ["*.java"],
        exclude = [
            "OperationsFinder.java",
            "EnrichedOperationBuilder.java",
        ],
    ),
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_proto",
        "@bazel_remote_apis//build/bazel/remote/execution/v2:remote_execution_java_grpc",
        "@com_google_googleapis//google/bytestream:bytestream_java_proto",
        "@com_google_googleapis//google/rpc:rpc_java_proto",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:com_google_protobuf_protobuf_java_util",
        "@maven//:com_googlecode_json_simple_json_simple",
        "@maven//:com_jayway_jsonpath_json_path",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_context",
        "@maven//:io_grpc_grpc_core",
        "@maven//:io_grpc_grpc_protobuf",
        "@maven//:io_grpc_grpc_stub",
    ],
)
