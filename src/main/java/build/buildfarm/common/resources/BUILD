load("@protobuf//bazel:java_proto_library.bzl", "java_proto_library")
load("@protobuf//bazel:proto_library.bzl", "proto_library")
load("@rules_java//java:java_library.bzl", "java_library")

package(default_visibility = ["//visibility:public"])

proto_library(
    name = "resource_proto",
    srcs = ["resource.proto"],
    deps = [
        "//src/main/protobuf/build/buildfarm/v1test:buildfarm_proto",
        "@googleapis//google/api:annotations_proto",
        "@googleapis//google/longrunning:operations_proto",
        "@googleapis//google/rpc:status_proto",
        "@protobuf//:duration_proto",
        "@protobuf//:timestamp_proto",
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
        "//src/main/protobuf/build/buildfarm/v1test:buildfarm_java_proto",
        "@buildfarm_maven//:com_github_jnr_jnr_constants",
        "@buildfarm_maven//:com_github_jnr_jnr_ffi",
        "@buildfarm_maven//:com_github_jnr_jnr_posix",
        "@buildfarm_maven//:com_google_code_findbugs_jsr305",
        "@buildfarm_maven//:com_google_guava_failureaccess",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java_util",
        "@buildfarm_maven//:commons_io_commons_io",
        "@buildfarm_maven//:io_grpc_grpc_api",
        "@buildfarm_maven//:io_grpc_grpc_context",
        "@buildfarm_maven//:io_grpc_grpc_protobuf",
        "@buildfarm_maven//:org_apache_commons_commons_compress",
        "@buildfarm_maven//:org_apache_commons_commons_lang3",
        "@buildfarm_maven//:org_projectlombok_lombok",
        "@buildfarm_maven//:org_threeten_threetenbp",
        "@buildfarm_maven//:redis_clients_jedis",
        "@googleapis//google/longrunning:longrunning_java_proto",
        "@googleapis//google/rpc:rpc_java_proto",
        "@remoteapis//build/bazel/remote/execution/v2:remote_execution_java_proto",
    ],
)
