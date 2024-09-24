load("@rules_java//java:defs.bzl", "java_proto_library")
load("@rules_proto//proto:defs.bzl", "proto_library")

package(default_visibility = ["//visibility:public"])

proto_library(
    name = "resource_proto",
    srcs = ["resource.proto"],
    deps = [
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_proto",
        "@com_google_protobuf//:duration_proto",
        "@com_google_protobuf//:timestamp_proto",
        "@googleapis//google/api:annotations_proto",
        "@googleapis//google/longrunning:operations_proto",
        "@googleapis//google/rpc:status_proto",
        "@remoteapis//build/bazel/remote/execution/v2:remote_execution_proto",
    ],
)

java_proto_library(
    name = "resource_java_proto",
    deps = [
        ":resource_proto",
    ],
)

java_library(
    name = "resources",
    srcs = glob(["*.java"]),
    plugins = [":lombok"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/build/buildfarm/common",
        "//src/main/java/build/buildfarm/common/resources:resource_java_proto",
        "//src/main/protobuf:build_buildfarm_v1test_buildfarm_java_proto",
        "@googleapis//google/longrunning:longrunning_java_proto",
        "@googleapis//google/rpc:rpc_java_proto",
        "@maven//:com_github_jnr_jnr_constants",
        "@maven//:com_github_jnr_jnr_ffi",
        "@maven//:com_github_jnr_jnr_posix",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_guava_failureaccess",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:com_google_protobuf_protobuf_java_util",
        "@maven//:commons_io_commons_io",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_context",
        "@maven//:io_grpc_grpc_protobuf",
        "@maven//:org_apache_commons_commons_compress",
        "@maven//:org_apache_commons_commons_lang3",
        "@maven//:org_projectlombok_lombok",
        "@maven//:org_threeten_threetenbp",
        "@maven//:redis_clients_jedis",
        "@remoteapis//build/bazel/remote/execution/v2:remote_execution_java_proto",
    ],
)
