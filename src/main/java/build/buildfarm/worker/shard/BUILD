load("@rules_java//java:java_library.bzl", "java_library")

java_library(
    name = "shard",
    srcs = glob(["*.java"]),
    plugins = ["//src/main/java/build/buildfarm/common:lombok"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/build/buildfarm/backplane",
        "//src/main/java/build/buildfarm/cas",
        "//src/main/java/build/buildfarm/common",
        "//src/main/java/build/buildfarm/common:BuildfarmExecutors",
        "//src/main/java/build/buildfarm/common/config",
        "//src/main/java/build/buildfarm/common/grpc",
        "//src/main/java/build/buildfarm/common/services",
        "//src/main/java/build/buildfarm/instance",
        "//src/main/java/build/buildfarm/instance/server",
        "//src/main/java/build/buildfarm/instance/shard",
        "//src/main/java/build/buildfarm/instance/stub",
        "//src/main/java/build/buildfarm/metrics/prometheus",
        "//src/main/java/build/buildfarm/worker",
        "//src/main/java/build/buildfarm/worker/cgroup",
        "//src/main/java/build/buildfarm/worker/resources",
        "//src/main/protobuf/build/buildfarm/v1test:buildfarm_java_grpc",
        "//src/main/protobuf/build/buildfarm/v1test:buildfarm_java_proto",
        "@buildfarm_maven//:com_github_ben_manes_caffeine_caffeine",
        "@buildfarm_maven//:com_github_pcj_google_options",
        "@buildfarm_maven//:com_google_code_findbugs_jsr305",
        "@buildfarm_maven//:com_google_guava_guava",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java",
        "@buildfarm_maven//:com_google_protobuf_protobuf_java_util",
        "@buildfarm_maven//:io_grpc_grpc_api",
        "@buildfarm_maven//:io_grpc_grpc_context",
        "@buildfarm_maven//:io_grpc_grpc_core",
        "@buildfarm_maven//:io_grpc_grpc_protobuf",
        "@buildfarm_maven//:io_grpc_grpc_services",
        "@buildfarm_maven//:io_grpc_grpc_stub",
        "@buildfarm_maven//:io_prometheus_simpleclient",
        "@buildfarm_maven//:javax_annotation_javax_annotation_api",
        "@buildfarm_maven//:org_projectlombok_lombok",
        "@googleapis//google/rpc:rpc_java_proto",
        "@io_grpc_grpc_proto//:health_java_proto",
    ],
)
