package(default_visibility = ["//visibility:public"])

load("//third_party/grpc:build_defs.bzl", "java_grpc_library")

proto_library(
    name = "build_buildfarm_v1test_buildfarm_proto",
    srcs = ["build/buildfarm/v1test/buildfarm.proto"],
    deps = [
        "@com_google_protobuf//:well_known_types_duration_proto",
        "@googleapis//:google_api_annotations_proto",
        "@googleapis//:google_devtools_remoteexecution_v1test_remote_execution_proto",
        "@googleapis//:google_longrunning_operations_proto",
        "@googleapis//:google_rpc_status_proto",
    ],
)

java_proto_library(
    name = "build_buildfarm_v1test_buildfarm_java_proto",
    deps = [
        ":build_buildfarm_v1test_buildfarm_proto",
        "@googleapis//:google_devtools_remoteexecution_v1test_remote_execution_proto",
        "@googleapis//:google_longrunning_operations_proto",
        "@googleapis//:google_rpc_status_proto",
    ],
)

java_grpc_library(
    name = "build_buildfarm_v1test_buildfarm_java_grpc",
    srcs = [":build_buildfarm_v1test_buildfarm_proto"],
    deps = [
        ":build_buildfarm_v1test_buildfarm_java_proto",
    ],
)
