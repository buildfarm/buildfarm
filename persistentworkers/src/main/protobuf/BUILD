load("@bazel_tools//tools/build_rules:utilities.bzl", "java_library_srcs")
load("@com_google_protobuf//bazel:java_proto_library.bzl", "java_proto_library")
load("@com_google_protobuf//bazel:proto_library.bzl", "proto_library")

package(default_visibility = ["//visibility:public"])

FILES = [
    "worker_protocol",
]

[proto_library(
    name = s + "_proto",
    srcs = [s + ".proto"],
) for s in FILES]

[java_proto_library(
    name = s + "_java_proto",
    deps = [":" + s + "_proto"],
) for s in FILES]

[java_library_srcs(
    name = s + "_java_proto_srcs",
    deps = [":" + s + "_java_proto"],
) for s in FILES]

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)

filegroup(
    name = "dist_jars",
    srcs = [s + "_java_proto_srcs" for s in FILES],
)
