load("@grpc-java//:java_grpc_library.bzl", "java_grpc_library")
load("@protobuf//bazel:java_proto_library.bzl", "java_proto_library")
load("@protobuf//bazel:proto_library.bzl", "proto_library")

package(default_visibility = ["//visibility:public"])

proto_library(
    name = "buildfarm_proto",
    srcs = ["buildfarm.proto"],
    deps = [
        "@googleapis//google/api:annotations_proto",
        "@googleapis//google/api:client_proto",
        "@googleapis//google/longrunning:operations_proto",
        "@googleapis//google/rpc:status_proto",
        "@protobuf//:duration_proto",
        "@protobuf//:timestamp_proto",
        "@remoteapis//build/bazel/remote/execution/v2:remote_execution_proto",
    ],
)

java_proto_library(
    name = "buildfarm_java_proto",
    deps = [
        ":buildfarm_proto",
        "@googleapis//google/longrunning:operations_proto",
        "@googleapis//google/rpc:status_proto",
        "@remoteapis//build/bazel/remote/execution/v2:remote_execution_proto",
    ],
)

java_grpc_library(
    name = "buildfarm_java_grpc",
    srcs = [":buildfarm_proto"],
    deps = [":buildfarm_java_proto"],
)
