load("@rules_java//java:java_library.bzl", "java_library")

java_library(
    name = "bazel-persistent-workers",
    srcs = glob(["**/*.java"]),
    plugins = ["//src/main/java/build/buildfarm/common:lombok"],
    visibility = ["//visibility:public"],
    deps = [
        "//persistentworkers/src/main/java/persistent/common:persistent-common",
        "//persistentworkers/src/main/protobuf:worker_protocol_java_proto",
        "//src/main/java/build/buildfarm/common",
        "@maven//:com_github_pcj_google_options",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:com_google_protobuf_protobuf_java_util",
        "@maven//:commons_io_commons_io",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_context",
        "@maven//:io_grpc_grpc_core",
        "@maven//:io_grpc_grpc_netty",
        "@maven//:io_grpc_grpc_protobuf",
        "@maven//:io_grpc_grpc_stub",
        "@maven//:org_apache_commons_commons_pool2",
        "@maven//:org_projectlombok_lombok",
    ],
)
